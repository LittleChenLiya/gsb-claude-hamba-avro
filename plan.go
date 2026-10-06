package avro

import (
	"fmt"
	"io"
	"slices"
	"strings"
)

// FieldAction describes what a compiled Plan does for a single bound field.
type FieldAction int

const (
	// FieldActionRead reads the writer value into the bound reader field.
	FieldActionRead FieldAction = iota
	// FieldActionPromote reads the writer value into the bound reader field,
	// applying a numeric (or string/bytes) promotion allowed by the Avro
	// schema resolution rules.
	FieldActionPromote
	// FieldActionDefault fills the reader field with its declared default,
	// as the writer schema does not contain the field.
	FieldActionDefault
	// FieldActionSkip skips the writer field, as the reader schema does not
	// contain the field.
	FieldActionSkip
)

// String returns a human-readable representation of the action.
func (a FieldAction) String() string {
	switch a {
	case FieldActionRead:
		return "read"
	case FieldActionPromote:
		return "promote"
	case FieldActionDefault:
		return "default"
	case FieldActionSkip:
		return "skip"
	default:
		return "unknown"
	}
}

// FieldBinding is a single bound field path of a compiled Plan.
//
// Fields are bound by record and field names and aliases, never by field
// position. Depending on the Action, either WriterPath or ReaderPath may be
// empty: a skip binding has no reader path, a default binding has no writer
// path.
type FieldBinding struct {
	// WriterPath is the dotted path of the field in the writer schema.
	WriterPath string
	// ReaderPath is the dotted path of the field in the reader schema.
	ReaderPath string
	// Action is the action performed for the field.
	Action FieldAction
	// WriterType is the Avro type of the writer field, if any.
	WriterType Type
	// ReaderType is the Avro type of the reader field, if any.
	ReaderType Type
}

// AliasConflictError is returned by CompilePlan when field matching by name
// and alias is ambiguous, e.g. when two reader fields claim the same writer
// field name through their aliases. It is diagnosed separately from schema
// incompatibility.
type AliasConflictError struct {
	// Record is the full name of the reader record containing the conflict.
	Record string
	// Name is the conflicting field name or alias.
	Name string
	// Fields are the reader fields claiming Name.
	Fields []string
}

// Error implements the error interface.
func (e *AliasConflictError) Error() string {
	return fmt.Sprintf("avro: plan: record %s: name or alias %q is claimed by multiple reader fields: %s",
		e.Record, e.Name, strings.Join(e.Fields, ", "))
}

// IncompatibleSchemaError is returned by CompilePlan when the reader schema
// cannot resolve the writer schema, e.g. a field type cannot be promoted or
// a required reader field has no default. It is diagnosed separately from
// alias ambiguity.
type IncompatibleSchemaError struct {
	// Reason is a human-readable description of the incompatibility.
	Reason string
}

// Error implements the error interface.
func (e *IncompatibleSchemaError) Error() string {
	return "avro: plan: incompatible schemas: " + e.Reason
}

// Plan is an explicitly compiled writer-reader resolution plan.
//
// A Plan is compiled from a writer and a reader schema without reading any
// data. Fields are bound by record and field names and aliases, filling
// reader defaults and applying only the promotions allowed by the Avro
// specification; fields are never copied by position. Execution follows only
// the field paths bound at compile time, and the plan holds its own resolved
// schema snapshot, so later modification of the reader schema does not
// affect previously compiled plans.
type Plan struct {
	schema   Schema
	bindings []FieldBinding
}

// CompilePlan compiles an explicit writer-reader resolution plan.
//
// Compilation only inspects the given schemas; it never reads data. The
// writer and reader schemas must both be records. Alias ambiguity is
// reported as *AliasConflictError, while type or schema incompatibility is
// reported as *IncompatibleSchemaError.
func CompilePlan(writer, reader Schema) (*Plan, error) {
	if writer == nil || reader == nil {
		return nil, &IncompatibleSchemaError{Reason: "writer and reader schemas must not be nil"}
	}

	w := resolveRefSchema(writer)
	r := resolveRefSchema(reader)
	if w.Type() != Record || r.Type() != Record {
		return nil, &IncompatibleSchemaError{
			Reason: fmt.Sprintf("writer and reader schemas must both be records, got %s and %s", w.Type(), r.Type()),
		}
	}

	// Diagnose alias ambiguity before and separately from incompatibility.
	if err := checkAliasAmbiguity(r.(*RecordSchema), w.(*RecordSchema)); err != nil {
		return nil, err
	}

	compat := NewSchemaCompatibility()
	if err := compat.Compatible(reader, writer); err != nil {
		return nil, &IncompatibleSchemaError{Reason: err.Error()}
	}

	// Resolve builds a composite schema snapshot with new field instances;
	// the plan does not retain a reference to the reader schema, so later
	// reader schema modification does not affect the compiled plan.
	resolved, err := compat.Resolve(reader, writer)
	if err != nil {
		return nil, &IncompatibleSchemaError{Reason: err.Error()}
	}

	p := &Plan{schema: resolved}
	p.bindings = buildBindings(r.(*RecordSchema), w.(*RecordSchema), "", "")

	return p, nil
}

// Schema returns the resolved schema snapshot the plan executes with.
func (p *Plan) Schema() Schema {
	return p.schema
}

// Bindings returns the field bindings compiled into the plan.
func (p *Plan) Bindings() []FieldBinding {
	bindings := make([]FieldBinding, len(p.bindings))
	copy(bindings, p.bindings)
	return bindings
}

// Unmarshal decodes data written with the writer schema into v, following
// only the field paths bound at compile time.
func (p *Plan) Unmarshal(data []byte, v any) error {
	return DefaultConfig.Unmarshal(p.schema, data, v)
}

// NewDecoder returns a new Decoder reading writer-schema data from r using
// the compiled plan.
func (p *Plan) NewDecoder(r io.Reader) *Decoder {
	return DefaultConfig.NewDecoder(p.schema, r)
}

// resolveRefSchema resolves reference schemas to the schema they point to.
func resolveRefSchema(s Schema) Schema {
	if s.Type() == Ref {
		return s.(*RefSchema).Schema()
	}
	return s
}

// checkAliasAmbiguity diagnoses ambiguous name/alias claims between the
// reader and writer records, recursing into nested record fields.
func checkAliasAmbiguity(reader, writer *RecordSchema) error {
	claims := map[string][]string{}
	for _, rf := range reader.Fields() {
		keys := make([]string, 0, len(rf.Aliases())+1)
		keys = append(keys, rf.Name())
		keys = append(keys, rf.Aliases()...)
		for _, key := range keys {
			if !slices.Contains(claims[key], rf.Name()) {
				claims[key] = append(claims[key], rf.Name())
			}
		}
	}
	for _, key := range sortedKeys(claims) {
		if len(claims[key]) > 1 {
			return &AliasConflictError{Record: reader.FullName(), Name: key, Fields: claims[key]}
		}
	}

	// Recurse into nested record fields bound by name or alias.
	for _, wf := range writer.Fields() {
		rf := findReaderField(reader, wf)
		if rf == nil {
			continue
		}
		wt, rt := resolveRefSchema(wf.Type()), resolveRefSchema(rf.Type())
		if wt.Type() == Record && rt.Type() == Record {
			if err := checkAliasAmbiguity(rt.(*RecordSchema), wt.(*RecordSchema)); err != nil {
				return err
			}
		}
	}

	return nil
}

// findReaderField returns the reader field bound to the writer field by name
// or by a reader field alias, or nil if there is no match. Ambiguity must
// have been ruled out by checkAliasAmbiguity beforehand.
func findReaderField(reader *RecordSchema, wf *Field) *Field {
	for _, rf := range reader.Fields() {
		if rf.Name() == wf.Name() {
			return rf
		}
	}
	for _, rf := range reader.Fields() {
		if slices.Contains(rf.Aliases(), wf.Name()) {
			return rf
		}
	}
	return nil
}

// buildBindings binds writer and reader record fields by name and alias,
// recording the action for each bound field path. Nested records are bound
// recursively with dotted paths.
func buildBindings(reader, writer *RecordSchema, writerPath, readerPath string) []FieldBinding {
	var bindings []FieldBinding

	matched := make(map[string]bool, len(reader.Fields()))
	for _, wf := range writer.Fields() {
		wp := joinPath(writerPath, wf.Name())
		wt := resolveRefSchema(wf.Type())

		rf := findReaderField(reader, wf)
		if rf == nil {
			// The writer field is not present in the reader; skip it.
			bindings = append(bindings, FieldBinding{
				WriterPath: wp,
				Action:     FieldActionSkip,
				WriterType: wt.Type(),
			})
			continue
		}

		matched[rf.Name()] = true
		rp := joinPath(readerPath, rf.Name())
		rt := resolveRefSchema(rf.Type())

		action := FieldActionRead
		if wt.Type() != rt.Type() && isPromotable(wt.Type(), rt.Type()) {
			action = FieldActionPromote
		}
		bindings = append(bindings, FieldBinding{
			WriterPath: wp,
			ReaderPath: rp,
			Action:     action,
			WriterType: wt.Type(),
			ReaderType: rt.Type(),
		})

		if wt.Type() == Record && rt.Type() == Record {
			bindings = append(bindings,
				buildBindings(rt.(*RecordSchema), wt.(*RecordSchema), wp, rp)...)
		}
	}

	for _, rf := range reader.Fields() {
		if matched[rf.Name()] {
			continue
		}
		// The reader field is not present in the writer; compatibility
		// guarantees it has a default to fill.
		bindings = append(bindings, FieldBinding{
			ReaderPath: joinPath(readerPath, rf.Name()),
			Action:     FieldActionDefault,
			ReaderType: resolveRefSchema(rf.Type()).Type(),
		})
	}

	return bindings
}

func joinPath(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "." + name
}

func sortedKeys(m map[string][]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
