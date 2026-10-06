package avro

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var errNoTypeConverter = errors.New("no type converter")

// TypeConverter represents a data type converter.
type TypeConverter interface {
	// Type returns the Avro data type of the type converter.
	Type() Type

	// LogicalType returns the Avro logical data type of the type converter.
	LogicalType() LogicalType

	// EncodeTypeConvert is the type conversion function called before encoding to Avro.
	EncodeTypeConvert(in any, schema Schema) (any, error)

	// DecodeTypeConvert is the type conversion function called after decoding from Avro.
	DecodeTypeConvert(in any, schema Schema) (any, error)
}

type specificType struct {
	typ Type
	lt  LogicalType
}

// TypeConverters holds the user-provided type conversion functions.
type TypeConverters struct {
	convs sync.Map // map[specificType]TypeConverter
	n     atomic.Int64
}

// has returns true when at least one type converter is registered.
func (c *TypeConverters) has() bool {
	return c.n.Load() > 0
}

// NewTypeConverters creates a new type converter.
func NewTypeConverters() *TypeConverters {
	return &TypeConverters{}
}

// RegisterTypeConverters registers type converters for converting the data types during encoding and decoding.
func (c *TypeConverters) RegisterTypeConverters(convs ...TypeConverter) {
	for _, conv := range convs {
		if typ := conv.Type(); len(typ) == 0 {
			continue
		}
		key := specificType{typ: conv.Type(), lt: conv.LogicalType()}
		if _, loaded := c.convs.LoadOrStore(key, conv); loaded {
			c.convs.Store(key, conv)
			continue
		}
		c.n.Add(1)
	}
}

// EncodeTypeConvert runs the encode type conversion function for the given value and schema.
func (c *TypeConverters) EncodeTypeConvert(in any, schema Schema) (any, error) {
	return c.EncodeTypeConvertContext(nil, in, schema)
}

// EncodeTypeConvertContext runs the encode type conversion function for the
// given value and schema, passing the explicit conversion path to converters
// implementing ContextTypeConverter. The conversion callback is run exactly
// once for the value. A conversion failure is reported with the path of the
// value and never returns a partial result.
func (c *TypeConverters) EncodeTypeConvertContext(path ConverterPath, in any, schema Schema) (any, error) {
	conv, ok := c.getTypeConverter(schema)
	if !ok {
		return in, errNoTypeConverter
	}

	if cconv, ok := conv.(ContextTypeConverter); ok {
		out, err := cconv.EncodeTypeConvertContext(converterContext(path, schema), in, schema)
		return out, conversionError(path, err)
	}

	out, err := conv.EncodeTypeConvert(in, schema)
	return out, conversionError(path, err)
}

// DecodeTypeConvert runs the decode type conversion function for the given value and schema.
func (c *TypeConverters) DecodeTypeConvert(in any, schema Schema) (any, error) {
	return c.DecodeTypeConvertContext(nil, in, schema)
}

// DecodeTypeConvertContext runs the decode type conversion function for the
// given value and schema, passing the explicit conversion path to converters
// implementing ContextTypeConverter. The conversion callback is run exactly
// once for the value. A conversion failure is reported with the path of the
// value and never returns a partial result.
func (c *TypeConverters) DecodeTypeConvertContext(path ConverterPath, in any, schema Schema) (any, error) {
	conv, ok := c.getTypeConverter(schema)
	if !ok {
		return in, errNoTypeConverter
	}

	if cconv, ok := conv.(ContextTypeConverter); ok {
		out, err := cconv.DecodeTypeConvertContext(converterContext(path, schema), in, schema)
		return out, conversionError(path, err)
	}

	out, err := conv.DecodeTypeConvert(in, schema)
	return out, conversionError(path, err)
}

// conversionError annotates a conversion error with the explicit path of the
// value that failed to convert.
func conversionError(path ConverterPath, err error) error {
	if err == nil || errors.Is(err, errNoTypeConverter) || len(path) == 0 {
		return err
	}
	return fmt.Errorf("avro: cannot convert value at %s: %w", path, err)
}

func (c *TypeConverters) getTypeConverter(schema Schema) (TypeConverter, bool) {
	typ := schema.Type()
	lt := getLogicalTypeOrProp(schema)
	conv, ok := c.convs.Load(specificType{typ: typ, lt: lt})
	if !ok {
		return nil, false
	}
	return conv.(TypeConverter), ok
}

// RegisterTypeConverters registers type converters for encoding and decoding the data type specified in the converter.
func RegisterTypeConverters(convs ...TypeConverter) {
	DefaultConfig.RegisterTypeConverters(convs...)
}

// TypeConversionFuncs is a helper struct to reduce boilerplate in user code.
//
// Implements the TypeConverter interface.
type TypeConversionFuncs struct {
	AvroType              Type
	AvroLogicalType       LogicalType
	EncoderTypeConversion func(in any, schema Schema) (any, error)
	DecoderTypeConversion func(in any, schema Schema) (any, error)
}

// Type returns the Avro data type of the type converter.
func (c TypeConversionFuncs) Type() Type {
	return c.AvroType
}

// LogicalType returns the Avro data type of the type converter.
func (c TypeConversionFuncs) LogicalType() LogicalType {
	return c.AvroLogicalType
}

// EncodeTypeConvert runs the converter's encoder type conversion function if it's set.
func (c TypeConversionFuncs) EncodeTypeConvert(in any, schema Schema) (any, error) {
	if c.EncoderTypeConversion == nil {
		return in, errNoTypeConverter
	}
	return c.EncoderTypeConversion(in, schema)
}

// DecodeTypeConvert runs the converter's decoder type conversion function if it's set.
func (c TypeConversionFuncs) DecodeTypeConvert(in any, schema Schema) (any, error) {
	if c.DecoderTypeConversion == nil {
		return in, errNoTypeConverter
	}
	return c.DecoderTypeConversion(in, schema)
}
