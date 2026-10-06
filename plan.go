package avro

import (
	"fmt"
	"io"
	"strings"
)

// BindingAction is the action a Plan takes for a writer field when resolving
// a writer schema against a reader schema.
type BindingAction int

const (
	// BindingBind binds the writer field to a reader field. The writer value
	// is decoded into the bound reader field using the field path recorded in
	// the binding.
	BindingBind BindingAction = iota

	// BindingIgnore ignores the writer field. No reader field matches the
	// writer field name or aliases, so the writer value is read and discarded.
	BindingIgnore

	// BindingDefault fills a reader field that has no matching writer field
	// with the reader field's default value.
	BindingDefault
)

// String returns the human-readable name of the binding action.
func (a BindingAction) String() string {
	switch a {
	case BindingBind:
		return "bind"
	case BindingIgnore:
		return "ignore"
	case BindingDefault:
		return "default"
	default:
		return "unknown"
	}
}

// FieldBinding describes how a single writer field is bound to a reader field
// by a Plan.
//
// A binding is explicit: it records the exact field path the plan follows at
// execution time. A BindingBind records both the writer field and the reader
// field it is bound to. A BindingIgnore records only the writer field. A
// BindingDefault records only the reader field that receives the default.
type FieldBinding struct {
	// WriterField is the field as it appears in the writer schema. It is nil
	// for BindingDefault bindings.
	WriterField *Field

	// ReaderField is the field as it appears in the reader schema. It is nil
	// for BindingIgnore bindings.
	ReaderField *Field

	// Action is the binding action the plan takes.
	Action BindingAction
}

// AmbiguityError indicates that a field name or alias resolves to more than
// one field, making the writer-to-reader resolution ambiguous.
//
// Ambiguity is diagnosed separately from schema incompatibility: a schema may
// be perfectly compatible yet still ambiguous because two reader fields claim
// the same writer field, or a single field claims multiple writer fields.
type AmbiguityError struct {
	// Name is the ambiguous field name or alias.
	Name string

	// Matches are the names of the fields the name or alias resolves to.
	Matches []string
}

func (e *AmbiguityError) Error() string {
	return fmt.Sprintf("avro: ambiguous field alias %q resolves to multiple fields: %s", e.Name, strings.Join(e.Matches, ", "))
}

// IncompatibilityError indicates that the writer and reader schemas are
// incompatible and cannot be resolved into a plan.
//
// Incompatibility is diagnosed separately from alias ambiguity: the schemas
// cannot be reconciled because their types do not match and no Avro promotion
// applies, or a required reader field is missing from the writer schema.
type IncompatibilityError struct {
	// WriterType is the type of the writer schema.
	WriterType string

	// ReaderType is the type of the reader schema.
	ReaderType string

	// Reason describes why the schemas are incompatible.
	Reason string
}

func (e *IncompatibilityError) Error() string {
	return fmt.Sprintf("avro: reader schema %s is not compatible with writer schema %s: %s", e.ReaderType, e.WriterType, e.Reason)
}

// Plan is an explicit, compiled writer-to-reader Avro schema resolution.
//
// A Plan is compiled from a writer schema and a reader schema and captures
// exactly how each writer field is bound to a reader field. Compilation never
// reads data; execution follows only the compiled bindings. Because the plan
// snapshots the reader schema at compile time, later modification of the
// reader schema does not affect plans already compiled.
//
// Use CompilePlan to create a plan, then Unmarshal to execute it.
type Plan struct {
	writer   Schema
	reader   Schema
	resolved Schema
	bindings []FieldBinding
}

// CompilePlan compiles a writer and reader schema into an explicit Plan.
//
// Compilation selects bindings by record and field aliases, default values,
// and the numeric promotions allowed by the Avro specification. It does not
// read any data. It returns an AmbiguityError if a field name or alias
// resolves to more than one field, and an IncompatibilityError if the schemas
// cannot be reconciled.
func CompilePlan(writer, reader Schema) (*Plan, error) {
	if writer == nil || reader == nil {
		return nil, &IncompatibilityError{
			WriterType: typeName(writer),
			ReaderType: typeName(reader),
			Reason:     "writer and reader schemas must not be nil",
		}
	}

	resolved, bindings, err := compileNode(writer, reader)
	if err != nil {
		return nil, err
	}

	return &Plan{
		writer:   writer,
		reader:   reader,
		resolved: resolved,
		bindings: bindings,
	}, nil
}

// Writer returns the writer schema the plan was compiled from.
func (p *Plan) Writer() Schema { return p.writer }

// Reader returns the reader schema the plan was compiled from.
func (p *Plan) Reader() Schema { return p.reader }

// Schema returns the resolved schema the plan executes against.
//
// The resolved schema is a snapshot built from the compiled bindings. It is
// safe to use with the standard Unmarshal and Decoder APIs.
func (p *Plan) Schema() Schema { return p.resolved }

// Bindings returns the explicit field bindings the plan compiled.
//
// The bindings describe, in writer field order, how each writer field is
// bound to a reader field, ignored, or filled with a default.
func (p *Plan) Bindings() []FieldBinding { return p.bindings }

// Unmarshal parses the Avro encoded data, written with the writer schema, and
// stores the result in the value pointed to by v, using the compiled plan.
//
// Execution follows only the field paths bound at compile time; unbound
// writer fields are read and discarded, and reader fields missing from the
// writer are filled with their defaults.
func (p *Plan) Unmarshal(data []byte, v any) error {
	return DefaultConfig.Unmarshal(p.resolved, data, v)
}

// NewDecoder returns a new decoder that reads from r using the compiled plan.
func (p *Plan) NewDecoder(r io.Reader) *Decoder {
	return NewDecoderForSchema(p.resolved, r)
}

// Decode reads the next Avro encoded value from r and stores it in v using the
// compiled plan.
func (p *Plan) Decode(r io.Reader, v any) error {
	return p.NewDecoder(r).Decode(v)
}

// compileNode resolves a writer schema against a reader schema and returns the
// resolved schema snapshot and the explicit field bindings. It mirrors the
// Avro specification's resolution rules: matching by name and alias, default
// filling, and the allowed numeric promotions.
func compileNode(writer, reader Schema) (Schema, []FieldBinding, error) {
	if writer.Type() == Ref {
		writer = writer.(*RefSchema).Schema()
	}
	if reader.Type() == Ref {
		reader = reader.(*RefSchema).Schema()
	}

	if writer.Type() == reader.Type() {
		return compileSameType(writer, reader)
	}

	// The types differ. Apply the Avro promotion rules.
	if isPromotable(writer.Type(), reader.Type()) {
		r := NewPrimitiveSchema(reader.Type(), reader.(*PrimitiveSchema).Logical(),
			withWriterFingerprint(writer.Fingerprint()),
		)
		r.encodedType = writer.Type()
		return r, nil, nil
	}

	// Writer union: the reader must be compatible with every writer type.
	if writer.Type() == Union {
		schemas := make([]Schema, 0)
		for _, ws := range writer.(*UnionSchema).Types() {
			s, _, err := compileNode(ws, reader)
			if err != nil {
				return nil, nil, err
			}
			schemas = append(schemas, s)
		}
		s, err := NewUnionSchema(schemas, withWriterFingerprint(writer.Fingerprint()))
		if err != nil {
			return nil, nil, err
		}
		return s, nil, nil
	}

	// Reader union: the writer must match at least one reader type.
	if reader.Type() == Union {
		var lastErr error
		for _, rs := range reader.(*UnionSchema).Types() {
			s, _, err := compileNode(writer, rs)
			if err == nil {
				return s, nil, nil
			}
			lastErr = err
		}
		if lastErr != nil {
			return nil, nil, lastErr
		}
	}

	return nil, nil, &IncompatibilityError{
		WriterType: string(writer.Type()),
		ReaderType: string(reader.Type()),
		Reason:     "types do not match and no Avro promotion applies",
	}
}

// compileSameType resolves a writer schema against a reader schema of the same
// type.
func compileSameType(writer, reader Schema) (Schema, []FieldBinding, error) {
	switch writer.Type() {
	case Record:
		return compileRecord(writer.(*RecordSchema), reader.(*RecordSchema))

	case Array:
		items, _, err := compileNode(writer.(*ArraySchema).Items(), reader.(*ArraySchema).Items())
		if err != nil {
			return nil, nil, err
		}
		return NewArraySchema(items, withWriterFingerprint(writer.Fingerprint())), nil, nil

	case Map:
		values, _, err := compileNode(writer.(*MapSchema).Values(), reader.(*MapSchema).Values())
		if err != nil {
			return nil, nil, err
		}
		return NewMapSchema(values, withWriterFingerprint(writer.Fingerprint())), nil, nil

	case Union:
		schemas := make([]Schema, 0)
		for _, ws := range writer.(*UnionSchema).Types() {
			s, _, err := compileNode(ws, reader)
			if err != nil {
				return nil, nil, err
			}
			schemas = append(schemas, s)
		}
		s, err := NewUnionSchema(schemas, withWriterFingerprint(writer.Fingerprint()))
		if err != nil {
			return nil, nil, err
		}
		return s, nil, nil

	case Enum:
		s, err := compileEnum(writer.(*EnumSchema), reader.(*EnumSchema))
		return s, nil, err

	case Fixed:
		s, err := compileFixed(writer.(*FixedSchema), reader.(*FixedSchema))
		return s, nil, err

	default:
		// Primitive types are identical; reuse the reader schema, which is
		// immutable after construction.
		return reader, nil, nil
	}
}

// compileRecord resolves a writer record against a reader record by compiling
// explicit field bindings.
func compileRecord(writer, reader *RecordSchema) (Schema, []FieldBinding, error) {
	// Build the reader field index: every reader field name and alias maps to
	// its field. A collision here means two reader fields claim the same name
	// or alias, which is ambiguous.
	readerIndex := make(map[string]*Field)
	for _, rf := range reader.Fields() {
		for _, key := range fieldKeys(rf) {
			if existing, ok := readerIndex[key]; ok && existing != rf {
				return nil, nil, &AmbiguityError{
					Name:    key,
					Matches: []string{existing.Name(), rf.Name()},
				}
			}
			readerIndex[key] = rf
		}
	}

	resolvedFields := make([]*Field, 0, len(writer.Fields())+len(reader.Fields()))
	bound := make(map[string]*Field) // reader field name -> writer field bound to it
	bindings := make([]FieldBinding, 0, len(writer.Fields())+len(reader.Fields()))

	for _, wf := range writer.Fields() {
		matches := matchReaderFields(readerIndex, wf)
		if len(matches) > 1 {
			names := make([]string, len(matches))
			for i, m := range matches {
				names[i] = m.Name()
			}
			return nil, nil, &AmbiguityError{
				Name:    wf.Name(),
				Matches: names,
			}
		}

		if len(matches) == 0 {
			// The writer field has no matching reader field. Ignore it.
			f, _ := NewField(wf.Name(), wf.Type(), WithAliases(wf.Aliases()), WithOrder(wf.Order()))
			f.def = wf.def
			f.hasDef = wf.hasDef
			f.action = FieldIgnore
			resolvedFields = append(resolvedFields, f)
			bindings = append(bindings, FieldBinding{
				WriterField: wf,
				Action:      BindingIgnore,
			})
			continue
		}

		rf := matches[0]
		if existing, ok := bound[rf.Name()]; ok {
			// A reader field cannot be bound to two writer fields.
			return nil, nil, &AmbiguityError{
				Name:    rf.Name(),
				Matches: []string{existing.Name(), wf.Name()},
			}
		}
		bound[rf.Name()] = wf

		ft, _, err := compileNode(wf.Type(), rf.Type())
		if err != nil {
			return nil, nil, err
		}

		f, _ := NewField(rf.Name(), ft, WithAliases(rf.Aliases()), WithOrder(rf.Order()))
		f.def = rf.def
		f.hasDef = rf.hasDef
		resolvedFields = append(resolvedFields, f)
		bindings = append(bindings, FieldBinding{
			WriterField: wf,
			ReaderField: rf,
			Action:      BindingBind,
		})
	}

	// Reader fields that have no matching writer field are filled with their
	// defaults. A reader field with no default is incompatible.
	for _, rf := range reader.Fields() {
		if _, ok := bound[rf.Name()]; ok {
			continue
		}
		if !rf.HasDefault() {
			return nil, nil, &IncompatibilityError{
				WriterType: writer.FullName(),
				ReaderType: reader.FullName(),
				Reason:     fmt.Sprintf("reader field %q is missing in writer schema and has no default", rf.Name()),
			}
		}

		f, _ := NewField(rf.Name(), rf.Type(), WithAliases(rf.Aliases()), WithOrder(rf.Order()))
		f.def = rf.def
		f.hasDef = rf.hasDef
		f.action = FieldSetDefault
		resolvedFields = append(resolvedFields, f)
		bindings = append(bindings, FieldBinding{
			ReaderField: rf,
			Action:      BindingDefault,
		})
	}

	resolved, err := NewRecordSchema(reader.Name(), reader.Namespace(), resolvedFields,
		WithAliases(reader.Aliases()),
		withWriterFingerprint(writer.Fingerprint()),
	)
	if err != nil {
		return nil, nil, err
	}
	return resolved, bindings, nil
}

// fieldKeys returns the names a reader field can be matched by: its own name
// followed by its aliases.
func fieldKeys(f *Field) []string {
	keys := make([]string, 0, 1+len(f.Aliases()))
	keys = append(keys, f.Name())
	keys = append(keys, f.Aliases()...)
	return keys
}

// matchReaderFields returns the reader fields a writer field matches by name
// and alias. A writer field matches a reader field if the writer field's name
// or aliases appear in the reader field's keys (name and aliases).
func matchReaderFields(readerIndex map[string]*Field, wf *Field) []*Field {
	seen := make(map[string]struct{})
	matches := make([]*Field, 0, 2)

	for _, key := range fieldKeys(wf) {
		rf := readerIndex[key]
		if rf == nil {
			continue
		}
		if _, ok := seen[rf.Name()]; ok {
			continue
		}
		seen[rf.Name()] = struct{}{}
		matches = append(matches, rf)
	}

	return matches
}

// compileEnum resolves a writer enum against a reader enum.
func compileEnum(writer, reader *EnumSchema) (Schema, error) {
	if err := checkEnumCompatibility(writer, reader); err != nil {
		if reader.HasDefault() {
			enum, _ := NewEnumSchema(reader.Name(), reader.Namespace(), reader.Symbols(),
				WithAliases(reader.Aliases()),
				WithDefault(reader.Default()),
				withWriterFingerprint(writer.Fingerprint()),
			)
			enum.encodedSymbols = writer.Symbols()
			return enum, nil
		}
		return nil, err
	}

	if !identicalEnumSymbols(writer, reader) {
		enum, _ := NewEnumSchema(reader.Name(), reader.Namespace(), reader.Symbols(),
			WithAliases(reader.Aliases()),
			WithDefault(reader.Default()),
			withWriterFingerprint(writer.Fingerprint()),
		)
		enum.encodedSymbols = writer.Symbols()
		return enum, nil
	}

	return reader, nil
}

// checkEnumCompatibility checks that every writer symbol exists in the reader
// symbols.
func checkEnumCompatibility(writer, reader *EnumSchema) error {
	for _, symbol := range writer.Symbols() {
		if !containsSymbol(reader.Symbols(), symbol) {
			return &IncompatibilityError{
				WriterType: writer.FullName(),
				ReaderType: reader.FullName(),
				Reason:     fmt.Sprintf("reader is missing symbol %q", symbol),
			}
		}
	}
	return nil
}

func containsSymbol(symbols []string, symbol string) bool {
	for _, s := range symbols {
		if s == symbol {
			return true
		}
	}
	return false
}

func identicalEnumSymbols(writer, reader *EnumSchema) bool {
	if len(writer.Symbols()) != len(reader.Symbols()) {
		return false
	}
	for i := range writer.Symbols() {
		if writer.Symbols()[i] != reader.Symbols()[i] {
			return false
		}
	}
	return true
}

// compileFixed resolves a writer fixed against a reader fixed.
func compileFixed(writer, reader *FixedSchema) (Schema, error) {
	if writer.Size() != reader.Size() {
		return nil, &IncompatibilityError{
			WriterType: writer.FullName(),
			ReaderType: reader.FullName(),
			Reason:     fmt.Sprintf("fixed sizes do not match: writer %d, reader %d", writer.Size(), reader.Size()),
		}
	}
	return reader, nil
}

func typeName(s Schema) string {
	if s == nil {
		return "<nil>"
	}
	return string(s.Type())
}
