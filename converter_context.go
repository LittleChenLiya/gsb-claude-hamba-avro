package avro

import (
	"strconv"
	"strings"
)

// PathSegmentKind represents the kind of a path segment.
type PathSegmentKind int

const (
	// FieldSegment is a record field path segment.
	FieldSegment PathSegmentKind = iota
	// KeySegment is a map key path segment.
	KeySegment
	// IndexSegment is an array index path segment.
	IndexSegment
)

// PathSegment is a single segment of the path to a value being converted.
//
// Exactly one of Field, Key or Index is set, depending on the Kind.
type PathSegment struct {
	// Kind is the kind of the path segment.
	Kind PathSegmentKind

	// Field is the record field name, set when Kind is FieldSegment.
	Field string

	// Key is the map key, set when Kind is KeySegment.
	Key string

	// Index is the array index, set when Kind is IndexSegment.
	Index int
}

// String returns the string representation of the path segment.
func (s PathSegment) String() string {
	switch s.Kind {
	case KeySegment:
		return `["` + s.Key + `"]`
	case IndexSegment:
		return "[" + strconv.Itoa(s.Index) + "]"
	default:
		return s.Field
	}
}

// ConvertContext is the explicit context of a value being converted by a
// TypeConverter. It is built while the value is actually being decoded or
// encoded, so the Path always reflects the real position of the value and
// never leaks into the next decode or encode.
type ConvertContext struct {
	// Path is the path of the value from the root of the document.
	// Record fields, map keys and array indexes are recorded as they are
	// traversed. The path is empty for the root value.
	Path []PathSegment

	// Type is the underlying Avro type of the value, e.g. Int for a date.
	Type Type

	// LogicalType is the Avro logical type of the value, e.g. Date for an
	// int date. It is empty when the schema has no logical type. Unknown
	// logical types are preserved as declared in the schema.
	LogicalType LogicalType
}

// String returns the string representation of the path of the value being
// converted, e.g. `items[1].meta["key"].date`. It returns an empty string
// for the root value.
func (c *ConvertContext) String() string {
	return formatConvertPath(c.Path)
}

func formatConvertPath(path []PathSegment) string {
	var sb strings.Builder
	for _, seg := range path {
		switch seg.Kind {
		case FieldSegment:
			if sb.Len() > 0 {
				sb.WriteByte('.')
			}
			sb.WriteString(seg.Field)
		case KeySegment:
			sb.WriteString(`["`)
			sb.WriteString(seg.Key)
			sb.WriteString(`"]`)
		case IndexSegment:
			sb.WriteByte('[')
			sb.WriteString(strconv.Itoa(seg.Index))
			sb.WriteByte(']')
		}
	}
	return sb.String()
}

// ContextualDecodeTypeConverter is an optional interface a TypeConverter can
// implement to receive the explicit ConvertContext of each value while
// decoding. When implemented, it is called instead of DecodeTypeConvert.
type ContextualDecodeTypeConverter interface {
	// DecodeTypeConvertWithContext is the type conversion function called
	// after decoding from Avro, with the explicit context of the value.
	DecodeTypeConvertWithContext(ctx *ConvertContext, in any, schema Schema) (any, error)
}

// ContextualEncodeTypeConverter is an optional interface a TypeConverter can
// implement to receive the explicit ConvertContext of each value while
// encoding. When implemented, it is called instead of EncodeTypeConvert.
type ContextualEncodeTypeConverter interface {
	// EncodeTypeConvertWithContext is the type conversion function called
	// before encoding to Avro, with the explicit context of the value.
	EncodeTypeConvertWithContext(ctx *ConvertContext, in any, schema Schema) (any, error)
}

// newConvertContext builds a ConvertContext for the schema from the current
// path. The path is copied so later traversal cannot mutate a context that
// was already handed out.
func newConvertContext(path []PathSegment, schema Schema) *ConvertContext {
	if ref, ok := schema.(*RefSchema); ok {
		schema = ref.Schema()
	}

	p := make([]PathSegment, len(path))
	copy(p, path)

	return &ConvertContext{
		Path:        p,
		Type:        schema.Type(),
		LogicalType: getLogicalType(schema),
	}
}
