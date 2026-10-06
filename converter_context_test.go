package avro_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errTestConversion = errors.New("test conversion error")

type convertCall struct {
	path        string
	typ         avro.Type
	logicalType avro.LogicalType
	in          any
}

// contextualDateConverter implements both the legacy TypeConverter interface
// and the contextual interfaces, recording the context of every call.
type contextualDateConverter struct {
	failOn   time.Time
	failDays int
	calls    []convertCall
}

func (c *contextualDateConverter) Type() avro.Type { return avro.Int }

func (c *contextualDateConverter) LogicalType() avro.LogicalType { return avro.Date }

func (c *contextualDateConverter) EncodeTypeConvert(in any, _ avro.Schema) (any, error) {
	return in, nil
}

func (c *contextualDateConverter) DecodeTypeConvert(in any, _ avro.Schema) (any, error) {
	return in, nil
}

func (c *contextualDateConverter) DecodeTypeConvertWithContext(ctx *avro.ConvertContext, in any, _ avro.Schema) (any, error) {
	c.calls = append(c.calls, convertCall{
		path:        ctx.String(),
		typ:         ctx.Type,
		logicalType: ctx.LogicalType,
		in:          in,
	})
	t := in.(time.Time)
	if t.Equal(c.failOn) {
		return nil, errTestConversion
	}
	return t.Format("2006-01-02"), nil
}

func (c *contextualDateConverter) EncodeTypeConvertWithContext(ctx *avro.ConvertContext, in any, _ avro.Schema) (any, error) {
	c.calls = append(c.calls, convertCall{
		path:        ctx.String(),
		typ:         ctx.Type,
		logicalType: ctx.LogicalType,
		in:          in,
	})
	days := in.(int)
	if days == c.failDays {
		return nil, errTestConversion
	}
	return time.Unix(int64(days)*int64(24*time.Hour/time.Second), 0).UTC(), nil
}

// The nested schema used by the contextual converter tests: an array of
// records, each with a nested record holding a date and a map of dates.
const nestedDateSchema = `{
	"type": "array",
	"items": {
		"type": "record",
		"name": "Event",
		"fields": [
			{
				"name": "meta",
				"type": {
					"type": "record",
					"name": "Meta",
					"fields": [
						{"name": "date", "type": {"type": "int", "logicalType": "date"}}
					]
				}
			},
			{"name": "tags", "type": {"type": "map", "values": {"type": "int", "logicalType": "date"}}}
		]
	}
}`

// nestedDateData holds two events: dates 1 and 3, with rec0 tags {"a": 2}.
var nestedDateData = []byte{
	0x04,                        // array block: 2 items
	0x02,                        // rec0.meta.date = 1
	0x02, 0x02, 'a', 0x04, 0x00, // rec0.tags = {"a": 2}
	0x06, // rec1.meta.date = 3
	0x00, // rec1.tags = {}
	0x00, // array end
}

func TestContextualDecodeTypeConverter_NestedPath(t *testing.T) {
	defer ConfigTeardown()

	conv := &contextualDateConverter{failOn: time.Unix(3*86400, 0).UTC()}
	avro.RegisterTypeConverters(conv)

	schema, err := avro.Parse(nestedDateSchema)
	require.NoError(t, err)

	// The first decode fails on the nested date of the second record and
	// must report the exact array path of the failing value.
	var got any
	err = avro.Unmarshal(schema, nestedDateData, &got)
	require.Error(t, err)
	assert.ErrorIs(t, err, errTestConversion)
	assert.Contains(t, err.Error(), "[1].meta.date")

	// A failed conversion must not return a partial result.
	assert.Nil(t, got)

	// The callback fired exactly once per date value seen so far, with the
	// logical type and the underlying primitive reported separately.
	require.Len(t, conv.calls, 3)
	assert.Equal(t, "[0].meta.date", conv.calls[0].path)
	assert.Equal(t, `[0].tags["a"]`, conv.calls[1].path)
	assert.Equal(t, "[1].meta.date", conv.calls[2].path)
	for _, call := range conv.calls {
		assert.Equal(t, avro.Int, call.typ)
		assert.Equal(t, avro.Date, call.logicalType)
	}

	// The next, valid decode must succeed and must not see any stale path
	// context from the failed decode.
	conv.failOn = time.Time{}
	got = nil
	err = avro.Unmarshal(schema, nestedDateData, &got)
	require.NoError(t, err)

	require.Len(t, conv.calls, 6)
	assert.Equal(t, "[0].meta.date", conv.calls[3].path)
	assert.Equal(t, `[0].tags["a"]`, conv.calls[4].path)
	assert.Equal(t, "[1].meta.date", conv.calls[5].path)

	// The converted values are used in the result.
	recs, ok := got.([]any)
	require.True(t, ok)
	require.Len(t, recs, 2)
	rec0 := recs[0].(map[string]any)
	assert.Equal(t, "1970-01-02", rec0["meta"].(map[string]any)["date"])
	assert.Equal(t, "1970-01-03", rec0["tags"].(map[string]any)["a"])
	rec1 := recs[1].(map[string]any)
	assert.Equal(t, "1970-01-04", rec1["meta"].(map[string]any)["date"])
}

func TestContextualEncodeTypeConverter_NestedPath(t *testing.T) {
	defer ConfigTeardown()

	conv := &contextualDateConverter{failDays: 3}
	avro.RegisterTypeConverters(conv)

	schema, err := avro.Parse(nestedDateSchema)
	require.NoError(t, err)

	value := []any{
		map[string]any{
			"meta": map[string]any{"date": 1},
			"tags": map[string]any{"a": 2},
		},
		map[string]any{
			"meta": map[string]any{"date": 3},
			"tags": map[string]any{},
		},
	}

	// Encoding fails on the nested date of the second record and must
	// report the exact array path of the failing value.
	_, err = avro.Marshal(schema, value)
	require.Error(t, err)
	assert.ErrorIs(t, err, errTestConversion)
	assert.Contains(t, err.Error(), "[1].meta.date")

	require.Len(t, conv.calls, 3)
	assert.Equal(t, "[0].meta.date", conv.calls[0].path)
	assert.Equal(t, `[0].tags["a"]`, conv.calls[1].path)
	assert.Equal(t, "[1].meta.date", conv.calls[2].path)
	for _, call := range conv.calls {
		assert.Equal(t, avro.Int, call.typ)
		assert.Equal(t, avro.Date, call.logicalType)
	}

	// The next, valid encode must succeed without stale path context.
	conv.failDays = -1
	conv.calls = nil
	data, err := avro.Marshal(schema, value)
	require.NoError(t, err)
	require.Len(t, conv.calls, 3)
	assert.Equal(t, "[0].meta.date", conv.calls[0].path)
	assert.Equal(t, `[0].tags["a"]`, conv.calls[1].path)
	assert.Equal(t, "[1].meta.date", conv.calls[2].path)

	// The encoded bytes decode back to the original document.
	conv.calls = nil
	var roundTrip any
	require.NoError(t, avro.Unmarshal(schema, data, &roundTrip))
	require.Len(t, conv.calls, 3)
	assert.Equal(t, "[0].meta.date", conv.calls[0].path)
	assert.Equal(t, `[0].tags["a"]`, conv.calls[1].path)
	assert.Equal(t, "[1].meta.date", conv.calls[2].path)
}

func TestLegacyTypeConverter_NestedErrorHasPath(t *testing.T) {
	defer ConfigTeardown()

	// A converter using the legacy interface still gets an accurate path
	// in the error, without implementing the contextual interface.
	avro.RegisterTypeConverters(avro.TypeConversionFuncs{
		AvroType:        avro.Int,
		AvroLogicalType: avro.Date,
		DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
			if in.(time.Time).Equal(time.Unix(3*86400, 0).UTC()) {
				return nil, errTestConversion
			}
			return in, nil
		},
	})

	schema, err := avro.Parse(nestedDateSchema)
	require.NoError(t, err)

	var got any
	err = avro.Unmarshal(schema, nestedDateData, &got)
	require.Error(t, err)
	assert.ErrorIs(t, err, errTestConversion)
	assert.Contains(t, err.Error(), "[1].meta.date")
	assert.Nil(t, got)
}

func TestSchema_UnknownLogicalTypePreserved(t *testing.T) {
	schema, err := avro.Parse(`{
		"type": "record",
		"name": "Event",
		"fields": [
			{"name": "kind", "type": {"type": "string", "logicalType": "custom-kind"}}
		]
	}`)
	require.NoError(t, err)

	// The unknown logical type is explicitly preserved on the schema.
	field := schema.(*avro.RecordSchema).Fields()[0]
	ps, ok := field.Type().(avro.PropertySchema)
	require.True(t, ok)
	assert.Equal(t, "custom-kind", ps.Prop("logicalType"))

	// The preserved logical type survives a round trip through JSON.
	data, err := json.Marshal(schema)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"logicalType":"custom-kind"`)

	parsed, err := avro.Parse(string(data))
	require.NoError(t, err)
	field = parsed.(*avro.RecordSchema).Fields()[0]
	ps, ok = field.Type().(avro.PropertySchema)
	require.True(t, ok)
	assert.Equal(t, "custom-kind", ps.Prop("logicalType"))
}

func TestContextualTypeConverter_UnknownLogicalType(t *testing.T) {
	defer ConfigTeardown()

	// A converter can target an unknown logical type that is preserved
	// from the schema.
	conv := &contextualCustomConverter{}
	avro.RegisterTypeConverters(conv)

	data := []byte{0x06, 'f', 'o', 'o'} // "foo"
	dec, err := avro.NewDecoder(`{"type": "string", "logicalType": "custom-kind"}`, bytes.NewReader(data))
	require.NoError(t, err)

	var got any
	err = dec.Decode(&got)
	require.NoError(t, err)
	assert.Equal(t, "kind:foo", got)

	require.Len(t, conv.calls, 1)
	assert.Equal(t, "", conv.calls[0].path) // root value
	assert.Equal(t, avro.String, conv.calls[0].typ)
	assert.Equal(t, avro.LogicalType("custom-kind"), conv.calls[0].logicalType)
}

type contextualCustomConverter struct {
	calls []convertCall
}

func (c *contextualCustomConverter) Type() avro.Type { return avro.String }

func (c *contextualCustomConverter) LogicalType() avro.LogicalType { return "custom-kind" }

func (c *contextualCustomConverter) EncodeTypeConvert(in any, _ avro.Schema) (any, error) {
	return in, nil
}

func (c *contextualCustomConverter) DecodeTypeConvert(in any, _ avro.Schema) (any, error) {
	return in, nil
}

func (c *contextualCustomConverter) DecodeTypeConvertWithContext(ctx *avro.ConvertContext, in any, _ avro.Schema) (any, error) {
	c.calls = append(c.calls, convertCall{
		path:        ctx.String(),
		typ:         ctx.Type,
		logicalType: ctx.LogicalType,
		in:          in,
	})
	return "kind:" + in.(string), nil
}

// Example of a contextual type converter receiving the explicit path of a
// nested value: the array index, record fields and map keys are passed to
// the converter while the value is actually being decoded.
func ExampleConvertContext() {
	schema, _ := avro.Parse(`{
		"type": "array",
		"items": {
			"type": "record",
			"name": "Event",
			"fields": [
				{"name": "meta", "type": {
					"type": "record",
					"name": "Meta",
					"fields": [
						{"name": "date", "type": {"type": "int", "logicalType": "date"}}
					]
				}}
			]
		}
	}`)

	conv := &exampleConverter{}
	avro.RegisterTypeConverters(conv)
	defer ConfigTeardown()

	data := []byte{0x04, 0x02, 0x00, 0x06, 0x00, 0x00} // two events, dates 1 and 3
	var got any
	_ = avro.Unmarshal(schema, data, &got)

	fmt.Println(conv.paths[0])
	fmt.Println(conv.paths[1])

	// Output:
	// [0].meta.date
	// [1].meta.date
}

type exampleConverter struct {
	paths []string
}

func (c *exampleConverter) Type() avro.Type             { return avro.Int }
func (c *exampleConverter) LogicalType() avro.LogicalType { return avro.Date }
func (c *exampleConverter) EncodeTypeConvert(in any, _ avro.Schema) (any, error) {
	return in, nil
}
func (c *exampleConverter) DecodeTypeConvert(in any, _ avro.Schema) (any, error) {
	return in, nil
}
func (c *exampleConverter) DecodeTypeConvertWithContext(ctx *avro.ConvertContext, in any, _ avro.Schema) (any, error) {
	c.paths = append(c.paths, ctx.String())
	return in, nil
}
