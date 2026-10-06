package avro

import (
	"strconv"
	"strings"
)

// PathSegmentKind describes the kind of a single conversion path segment.
type PathSegmentKind int

const (
	// FieldSegment is a record field path segment.
	FieldSegment PathSegmentKind = iota
	// IndexSegment is an array index path segment.
	IndexSegment
	// KeySegment is a map key path segment.
	KeySegment
)

// PathSegment is a single step of a ConverterPath, locating a value within
// its containing record, array or map.
type PathSegment struct {
	// Kind describes which of Field, Index or Key is set.
	Kind PathSegmentKind
	// Field is the record field name. Set when Kind is FieldSegment.
	Field string
	// Index is the array index. Set when Kind is IndexSegment.
	Index int
	// Key is the map key. Set when Kind is KeySegment.
	Key string
}

// String returns the string representation of the path segment.
func (s PathSegment) String() string {
	switch s.Kind {
	case IndexSegment:
		return "[" + strconv.Itoa(s.Index) + "]"
	case KeySegment:
		return "[\"" + s.Key + "\"]"
	default:
		return s.Field
	}
}

// ConverterPath is the explicit location of a value within the structure
// being converted, built from record field names, array indices and map
// keys as they are traversed during conversion.
type ConverterPath []PathSegment

// String returns the string representation of the path, e.g.
// "events[1].when" or "labels[key]". An empty path returns "<root>".
func (p ConverterPath) String() string {
	if len(p) == 0 {
		return "<root>"
	}

	var sb strings.Builder
	for _, seg := range p {
		if seg.Kind == FieldSegment && sb.Len() > 0 {
			sb.WriteString(".")
		}
		sb.WriteString(seg.String())
	}
	return sb.String()
}

// Field appends a record field segment to the path.
func (p ConverterPath) Field(name string) ConverterPath {
	return append(p, PathSegment{Kind: FieldSegment, Field: name})
}

// Index appends an array index segment to the path.
func (p ConverterPath) Index(i int) ConverterPath {
	return append(p, PathSegment{Kind: IndexSegment, Index: i})
}

// Key appends a map key segment to the path.
func (p ConverterPath) Key(key string) ConverterPath {
	return append(p, PathSegment{Kind: KeySegment, Key: key})
}

// ConverterContext is the explicit context of a single value being
// converted by a ContextTypeConverter.
//
// The underlying primitive Avro type and the logical type of the value are
// reported separately. A logical type that is unknown to the library is
// explicitly preserved from the schema logicalType property.
type ConverterContext struct {
	// Path is the explicit location of the value within its containing
	// structure. It is empty for a top-level value.
	Path ConverterPath
	// Type is the underlying Avro type of the value, e.g. Int.
	Type Type
	// LogicalType is the logical type of the value, or empty when the
	// value has no logical type. Unknown logical types are preserved as
	// declared in the schema.
	LogicalType LogicalType
}

// String returns the string representation of the context, e.g.
// "events[1].when (int, logicalType date)".
func (c ConverterContext) String() string {
	var sb strings.Builder
	sb.WriteString(c.Path.String())
	sb.WriteString(" (")
	sb.WriteString(string(c.Type))
	if c.LogicalType != "" {
		sb.WriteString(", logicalType ")
		sb.WriteString(string(c.LogicalType))
	}
	sb.WriteString(")")
	return sb.String()
}

// ContextTypeConverter is an optional extension of the TypeConverter
// interface. A converter implementing it receives the explicit
// ConverterContext (path, primitive type and logical type) of every value
// being converted, exactly once per value.
//
// The TypeConverter interface remains fully compatible; converters that do
// not implement ContextTypeConverter are called without context.
type ContextTypeConverter interface {
	TypeConverter

	// EncodeTypeConvertContext is the type conversion function called before
	// encoding to Avro, with the explicit context of the value.
	EncodeTypeConvertContext(ctx ConverterContext, in any, schema Schema) (any, error)

	// DecodeTypeConvertContext is the type conversion function called after
	// decoding from Avro, with the explicit context of the value.
	DecodeTypeConvertContext(ctx ConverterContext, in any, schema Schema) (any, error)
}

// TypeConversionContextFuncs is a helper struct to reduce boilerplate in
// user code, providing context-aware conversion functions.
//
// Implements both the TypeConverter and ContextTypeConverter interfaces.
type TypeConversionContextFuncs struct {
	AvroType              Type
	AvroLogicalType       LogicalType
	EncoderTypeConversion func(ctx ConverterContext, in any, schema Schema) (any, error)
	DecoderTypeConversion func(ctx ConverterContext, in any, schema Schema) (any, error)
}

// Type returns the Avro data type of the type converter.
func (c TypeConversionContextFuncs) Type() Type {
	return c.AvroType
}

// LogicalType returns the Avro logical data type of the type converter.
func (c TypeConversionContextFuncs) LogicalType() LogicalType {
	return c.AvroLogicalType
}

// EncodeTypeConvert runs the converter encoder type conversion function
// with an empty context.
func (c TypeConversionContextFuncs) EncodeTypeConvert(in any, schema Schema) (any, error) {
	return c.EncodeTypeConvertContext(ConverterContext{}, in, schema)
}

// DecodeTypeConvert runs the converter decoder type conversion function
// with an empty context.
func (c TypeConversionContextFuncs) DecodeTypeConvert(in any, schema Schema) (any, error) {
	return c.DecodeTypeConvertContext(ConverterContext{}, in, schema)
}

// EncodeTypeConvertContext runs the converter encoder type conversion
// function if it is set.
func (c TypeConversionContextFuncs) EncodeTypeConvertContext(ctx ConverterContext, in any, schema Schema) (any, error) {
	if c.EncoderTypeConversion == nil {
		return in, errNoTypeConverter
	}
	return c.EncoderTypeConversion(ctx, in, schema)
}

// DecodeTypeConvertContext runs the converter decoder type conversion
// function if it is set.
func (c TypeConversionContextFuncs) DecodeTypeConvertContext(ctx ConverterContext, in any, schema Schema) (any, error) {
	if c.DecoderTypeConversion == nil {
		return in, errNoTypeConverter
	}
	return c.DecoderTypeConversion(ctx, in, schema)
}

// getLogicalTypeOrProp returns the logical type of the schema, explicitly
// preserving an unknown logical type from the schema logicalType property.
func getLogicalTypeOrProp(schema Schema) LogicalType {
	if lt := getLogicalType(schema); lt != "" {
		return lt
	}

	ps, ok := schema.(PropertySchema)
	if !ok {
		return ""
	}
	if lt, ok := ps.Prop("logicalType").(string); ok {
		return LogicalType(lt)
	}
	return ""
}

// converterContext builds the explicit context of the value at path for the
// given schema, separating the underlying primitive type from the logical
// type. The path is cloned so later traversal cannot mutate the context.
func converterContext(path ConverterPath, schema Schema) ConverterContext {
	return ConverterContext{
		Path:        append(ConverterPath(nil), path...),
		Type:        schema.Type(),
		LogicalType: getLogicalTypeOrProp(schema),
	}
}

// pathTracker tracks the explicit conversion path on a Reader or Writer.
// Tracking is only performed when type converters are registered.
type pathTracker struct {
	cfg  *frozenConfig
	path ConverterPath
}

func (t *pathTracker) tracking() bool {
	return t.cfg != nil && t.cfg.typeConverters.has()
}

// pushPathField pushes a record field segment onto the path.
func (t *pathTracker) pushPathField(name string) {
	if !t.tracking() {
		return
	}
	t.path = t.path.Field(name)
}

// pushPathIndex pushes an array index segment placeholder onto the path.
// The index is updated with setPathIndex as elements are traversed.
func (t *pathTracker) pushPathIndex() {
	if !t.tracking() {
		return
	}
	t.path = t.path.Index(0)
}

// setPathIndex updates the index of the innermost path segment.
func (t *pathTracker) setPathIndex(i int) {
	if n := len(t.path); n > 0 && t.path[n-1].Kind == IndexSegment {
		t.path[n-1].Index = i
	}
}

// pushPathKey pushes a map key segment placeholder onto the path.
// The key is updated with setPathKey as entries are traversed.
func (t *pathTracker) pushPathKey() {
	if !t.tracking() {
		return
	}
	t.path = t.path.Key("")
}

// setPathKey updates the key of the innermost path segment.
func (t *pathTracker) setPathKey(key string) {
	if n := len(t.path); n > 0 && t.path[n-1].Kind == KeySegment {
		t.path[n-1].Key = key
	}
}

// popPath removes the innermost path segment.
func (t *pathTracker) popPath() {
	if n := len(t.path); n > 0 {
		t.path = t.path[:n-1]
	}
}

// resetPath clears the path so no context leaks into the next conversion.
func (t *pathTracker) resetPath() {
	t.path = t.path[:0]
}
