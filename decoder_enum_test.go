package avro_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoder_EnumInvalidType(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD}
	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got int
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_Enum(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02}
	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got string
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, "bar", got)
}

func TestDecoder_EnumInvalidSymbol(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04}
	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got string
	err := dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_EnumError(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD}
	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got string
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_EnumTextUnmarshaler(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02}
	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got *testEnumTextUnmarshaler
	err := dec.Decode(&got)

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, testEnumTextUnmarshaler(1), *got)
}

func TestDecoder_EnumTextUnmarshalerNonPtr(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02}
	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got testEnumTextUnmarshaler
	err := dec.Decode(&got)

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, testEnumTextUnmarshaler(1), got)
}

func TestDecoder_EnumTextUnmarshalerObj(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type":"enum", "name": "test1", "symbols": ["foo", "bar"]}}
    ]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got testEnumUnmarshalerObj
	err = dec.Decode(&got)

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, testEnumUnmarshalerObj{A: testEnumTextUnmarshaler(1)}, got)
}

func TestDecoder_EnumTextUnmarshalerInvalidSymbol(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04}
	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got *testEnumTextUnmarshaler
	err := dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_EnumTextUnmarshalerEnumError(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD}
	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got *testEnumTextUnmarshaler
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_EnumTextUnmarshalerError(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04}
	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar", "baz"]}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got *testEnumTextUnmarshaler
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_EnumResolved(t *testing.T) {
	defer ConfigTeardown()

	reader := avro.MustParse(`{"type":"enum", "name": "test", "symbols": ["A", "B"], "default": "A"}`)
	writer := avro.MustParse(`{"type":"enum", "name": "test", "symbols": ["X", "B"]}`)

	schema, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
	require.NoError(t, err)

	tests := []struct {
		name string
		data []byte
		want string
	}{
		// Writer symbols are mapped by name, not by position.
		{name: "known symbol", data: []byte{0x02}, want: "B"},
		// A writer symbol missing in the reader falls back to the reader default.
		{name: "unknown symbol with default", data: []byte{0x00}, want: "A"},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var got string
			err := avro.Unmarshal(schema, test.data, &got)

			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestDecoder_EnumResolvedUnknownSymbolWithoutDefault(t *testing.T) {
	defer ConfigTeardown()

	reader := avro.MustParse(`{"type":"enum", "name": "test", "symbols": ["A", "B"]}`)
	writer := avro.MustParse(`{"type":"enum", "name": "test", "symbols": ["X", "B"]}`)

	// The resolution plan must be rejected before any data is consumed.
	_, err := avro.NewSchemaCompatibility().Resolve(reader, writer)

	assert.Error(t, err)
}

func TestDecoder_EnumResolvedDefaultMustBeReaderSymbol(t *testing.T) {
	defer ConfigTeardown()

	// A default that is not a declared reader symbol is rejected while
	// parsing the schema, not while decoding data.
	_, err := avro.Parse(`{"type":"enum", "name": "test", "symbols": ["A", "B"], "default": "X"}`)

	assert.Error(t, err)
}

func TestDecoder_EnumResolvedTextUnmarshaler(t *testing.T) {
	defer ConfigTeardown()

	reader := avro.MustParse(`{"type":"enum", "name": "test", "symbols": ["foo", "bar"], "default": "foo"}`)
	writer := avro.MustParse(`{"type":"enum", "name": "test", "symbols": ["baz", "bar"]}`)

	schema, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
	require.NoError(t, err)

	tests := []struct {
		name string
		data []byte
		want testEnumTextUnmarshaler
	}{
		{name: "known symbol", data: []byte{0x02}, want: testEnumTextUnmarshaler(1)},
		{name: "unknown symbol with default", data: []byte{0x00}, want: testEnumTextUnmarshaler(0)},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var got testEnumTextUnmarshaler
			err := avro.Unmarshal(schema, test.data, &got)

			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

type testEnumUnmarshalerObj struct {
	A testEnumTextUnmarshaler `avro:"a"`
}

type testEnumTextUnmarshaler int

func (m *testEnumTextUnmarshaler) UnmarshalText(data []byte) error {
	switch string(data) {
	case "foo":
		*m = 0
		return nil
	case "bar":
		*m = 1
		return nil
	default:
		return errors.New("unknown symbol")
	}
}
