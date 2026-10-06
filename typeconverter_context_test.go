package avro_test

import (
	"errors"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// contextEventsSchema is a record holding an array of records, each with a
// nested date field.
const contextEventsSchema = `{
	"type": "record",
	"name": "Log",
	"fields": [
		{"name": "events", "type": {"type": "array", "items": {
			"type": "record",
			"name": "Event",
			"fields": [
				{"name": "id", "type": "long"},
				{"name": "when", "type": {"type": "int", "logicalType": "date"}}
			]
		}}}
	]
}`

// dateContextRecorder is a path-aware date converter recording every context
// it is called with and failing for a specific epoch day.
type dateContextRecorder struct {
	contexts []avro.ConverterContext
	failDay  int64
}

func (r *dateContextRecorder) converter() avro.TypeConversionContextFuncs {
	return avro.TypeConversionContextFuncs{
		AvroType:        avro.Int,
		AvroLogicalType: avro.Date,
		DecoderTypeConversion: func(ctx avro.ConverterContext, in any, _ avro.Schema) (any, error) {
			r.contexts = append(r.contexts, ctx)
			days := in.(time.Time).Unix() / 86400
			if days == r.failDay {
				return nil, errors.New("invalid date")
			}
			return days, nil
		},
	}
}

func TestDecoderTypeConverterContext_NestedArrayRecordPath(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(contextEventsSchema)

	rec := &dateContextRecorder{failDay: 5}
	avro.RegisterTypeConverters(rec.converter())

	// Two records; the nested date of the second record fails to convert.
	bad := []byte{0x04, 0x02, 0x00, 0x04, 0x0a, 0x00}
	var got any
	err := avro.Unmarshal(schema, bad, &got)
	require.Error(t, err)
	assert.ErrorContains(t, err, "events[1].when")
	assert.ErrorContains(t, err, "invalid date")

	// The conversion callback ran exactly once per value, with the explicit
	// path of each value.
	require.Len(t, rec.contexts, 2)
	assert.Equal(t, "events[0].when", rec.contexts[0].Path.String())
	assert.Equal(t, "events[1].when", rec.contexts[1].Path.String())

	// The underlying primitive type and the logical type are reported
	// separately.
	assert.Equal(t, avro.Int, rec.contexts[1].Type)
	assert.Equal(t, avro.Date, rec.contexts[1].LogicalType)

	// The failed conversion did not produce a partial result.
	assert.Nil(t, got)

	// The next valid decode does not see any stale path from the failed one.
	rec.contexts = nil
	rec.failDay = -1
	good := []byte{0x04, 0x02, 0x00, 0x04, 0x00, 0x00}
	err = avro.Unmarshal(schema, good, &got)
	require.NoError(t, err)
	require.Len(t, rec.contexts, 2)
	assert.Equal(t, "events[0].when", rec.contexts[0].Path.String())
	assert.Equal(t, "events[1].when", rec.contexts[1].Path.String())
	assert.Equal(t, map[string]any{"events": []any{
		map[string]any{"id": int64(1), "when": int64(0)},
		map[string]any{"id": int64(2), "when": int64(0)},
	}}, got)
}

func TestDecoderTypeConverterContext_MapKeyPath(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{
		"type": "record",
		"name": "Log",
		"fields": [
			{"name": "labels", "type": {"type": "map", "values": {"type": "int", "logicalType": "date"}}}
		]
	}`)

	rec := &dateContextRecorder{failDay: 5}
	avro.RegisterTypeConverters(rec.converter())

	// The value of map key "b" fails to convert.
	data := []byte{0x04, 0x02, 0x61, 0x00, 0x02, 0x62, 0x0a, 0x00}
	var got any
	err := avro.Unmarshal(schema, data, &got)
	require.Error(t, err)
	assert.ErrorContains(t, err, "labels[\"b\"]")

	require.Len(t, rec.contexts, 2)
	assert.Equal(t, "labels[\"a\"]", rec.contexts[0].Path.String())
	assert.Equal(t, "labels[\"b\"]", rec.contexts[1].Path.String())
}

func TestDecoderTypeConverterContext_StructTarget(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{
		"type": "record",
		"name": "Log",
		"fields": [
			{"name": "Events", "type": {"type": "array", "items": {
				"type": "record",
				"name": "Event",
				"fields": [
					{"name": "id", "type": "long"},
					{"name": "when", "type": {"type": "int", "logicalType": "date"}}
				]
			}}}
		]
	}`)

	rec := &dateContextRecorder{failDay: -1}
	avro.RegisterTypeConverters(rec.converter())

	type Log struct {
		Events []any
	}
	var got Log
	err := avro.Unmarshal(schema, []byte{0x04, 0x02, 0x00, 0x04, 0x00, 0x00}, &got)
	require.NoError(t, err)
	require.Len(t, rec.contexts, 2)
	assert.Equal(t, "Events[0].when", rec.contexts[0].Path.String())
	assert.Equal(t, "Events[1].when", rec.contexts[1].Path.String())
}

func TestDecoderTypeConverterContext_UnknownLogicalTypePreserved(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{
		"type": "record",
		"name": "Log",
		"fields": [
			{"name": "v", "type": {"type": "int", "logicalType": "custom-thing"}}
		]
	}`)

	// The unknown logical type is explicitly preserved on the schema.
	field := schema.(*avro.RecordSchema).Fields()[0]
	propSchema, ok := field.Type().(avro.PropertySchema)
	require.True(t, ok)
	assert.Equal(t, "custom-thing", propSchema.Prop("logicalType"))

	var contexts []avro.ConverterContext
	avro.RegisterTypeConverters(avro.TypeConversionContextFuncs{
		AvroType:        avro.Int,
		AvroLogicalType: "custom-thing",
		DecoderTypeConversion: func(ctx avro.ConverterContext, in any, _ avro.Schema) (any, error) {
			contexts = append(contexts, ctx)
			return "custom", nil
		},
	})

	var got any
	err := avro.Unmarshal(schema, []byte{0x04}, &got)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"v": "custom"}, got)

	// The converter matched the unknown logical type, and the context kept
	// the logical type separate from the underlying primitive type.
	require.Len(t, contexts, 1)
	assert.Equal(t, avro.Int, contexts[0].Type)
	assert.Equal(t, avro.LogicalType("custom-thing"), contexts[0].LogicalType)
	assert.Equal(t, "v", contexts[0].Path.String())
}

func TestDecoderTypeConverterContext_PlainConverterCompatible(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(contextEventsSchema)

	testError := errors.New("test error")
	avro.RegisterTypeConverters(avro.TypeConversionFuncs{
		AvroType:        avro.Int,
		AvroLogicalType: avro.Date,
		DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
			if in.(time.Time).Unix() != 0 {
				return nil, testError
			}
			return in, nil
		},
	})

	// A converter implementing only the original TypeConverter interface
	// still works, and its error is reported with the path of the value.
	bad := []byte{0x04, 0x02, 0x00, 0x04, 0x0a, 0x00}
	var got any
	err := avro.Unmarshal(schema, bad, &got)
	require.Error(t, err)
	assert.ErrorIs(t, err, testError)
	assert.ErrorContains(t, err, "events[1].when")
}

func TestEncoderTypeConverterContext_NestedPath(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{
		"type": "record",
		"name": "Log",
		"fields": [
			{"name": "items", "type": {"type": "array", "items": {"type": "int", "logicalType": "date"}}}
		]
	}`)

	var contexts []avro.ConverterContext
	avro.RegisterTypeConverters(avro.TypeConversionContextFuncs{
		AvroType:        avro.Int,
		AvroLogicalType: avro.Date,
		EncoderTypeConversion: func(ctx avro.ConverterContext, in any, _ avro.Schema) (any, error) {
			contexts = append(contexts, ctx)
			return int(in.(time.Time).Unix() / 86400), nil
		},
	})

	val := map[string]any{
		"items": []any{
			time.Unix(0, 0).UTC(),
			time.Unix(5*86400, 0).UTC(),
		},
	}
	data, err := avro.Marshal(schema, val)
	require.NoError(t, err)
	assert.Equal(t, []byte{0x03, 0x04, 0x00, 0x0a, 0x00}, data)

	require.Len(t, contexts, 2)
	assert.Equal(t, "items[0]", contexts[0].Path.String())
	assert.Equal(t, "items[1]", contexts[1].Path.String())
	assert.Equal(t, avro.Int, contexts[0].Type)
	assert.Equal(t, avro.Date, contexts[0].LogicalType)
}

func TestConverterPath_String(t *testing.T) {
	assert.Equal(t, "<root>", avro.ConverterPath{}.String())

	p := avro.ConverterPath{}.
		Field("events").
		Index(1).
		Field("when")
	assert.Equal(t, "events[1].when", p.String())

	p = avro.ConverterPath{}.
		Field("labels").
		Key("b")
	assert.Equal(t, "labels[\"b\"]", p.String())

	p = avro.ConverterPath{}.Index(3)
	assert.Equal(t, "[3]", p.String())
}

func TestConverterContext_String(t *testing.T) {
	ctx := avro.ConverterContext{
		Path:        avro.ConverterPath{}.Field("events").Index(1).Field("when"),
		Type:        avro.Int,
		LogicalType: avro.Date,
	}
	assert.Equal(t, "events[1].when (int, logicalType date)", ctx.String())

	ctx = avro.ConverterContext{Type: avro.String}
	assert.Equal(t, "<root> (string)", ctx.String())
}
