package avro_test

import (
	"math/big"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSchemaCompatibility_StrictLogicalTypesAcceptance drives the three
// acceptance scenarios entirely in memory: an Avro payload is encoded with the
// writer schema and a decode plan is compiled (Resolve) against the reader
// schema, both with strict logical type matching enabled and with the default
// (unchanged) compatibility rules.
func TestSchemaCompatibility_StrictLogicalTypesAcceptance(t *testing.T) {
	type decimalRecord struct {
		A int      `avro:"a"`
		B *big.Rat `avro:"b"`
	}

	readerDecimalScale2 := `{
		"type":"record","name":"test","namespace":"org.hamba.avro",
		"fields":[
			{"name":"a","type":"int"},
			{"name":"b","type":{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}}
		]}`
	writerDecimalScale3 := `{
		"type":"record","name":"test","namespace":"org.hamba.avro",
		"fields":[
			{"name":"a","type":"int"},
			{"name":"b","type":{"type":"bytes","logicalType":"decimal","precision":4,"scale":3}}
		]}`
	writerPlainBytes := `{
		"type":"record","name":"test","namespace":"org.hamba.avro",
		"fields":[
			{"name":"a","type":"int"},
			{"name":"b","type":"bytes"}
		]}`
	reader := avro.MustParse(readerDecimalScale2)

	t.Run("two field record scale conflict rejects strict plan", func(t *testing.T) {
		writer := avro.MustParse(writerDecimalScale3)

		payload, err := avro.Marshal(writer, decimalRecord{
			A: 7,
			B: big.NewRat(123, 100), // 1.230 encoded unscaled at scale 3
		})
		require.NoError(t, err)

		_, err = avro.NewSchemaCompatibility(avro.WithStrictLogicalTypes()).Resolve(reader, writer)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "reader field path b")
		assert.Contains(t, err.Error(), "writer field path b")
		assert.Contains(t, err.Error(), "decimal(4,2)")
		assert.Contains(t, err.Error(), "decimal(4,3)")

		// The default mode keeps the original structural rules and the plan compiles.
		schema, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
		require.NoError(t, err)

		var got decimalRecord
		require.NoError(t, avro.Unmarshal(schema, payload, &got))
		assert.Equal(t, 7, got.A)
		// Unscaled bytes 1230 read at reader scale 2 yields 12.3, not 1.23:
		// exactly the precision/scale divergence strict mode guards against.
		assert.Equal(t, big.NewRat(123, 10), got.B)
	})

	t.Run("matching decimal round trips under strict plan", func(t *testing.T) {
		writer := reader

		payload, err := avro.Marshal(writer, decimalRecord{
			A: 8,
			B: big.NewRat(123, 100), // 1.23
		})
		require.NoError(t, err)

		schema, err := avro.NewSchemaCompatibility(avro.WithStrictLogicalTypes()).Resolve(reader, writer)
		require.NoError(t, err)

		var got decimalRecord
		require.NoError(t, avro.Unmarshal(schema, payload, &got))
		assert.Equal(t, 8, got.A)
		assert.Equal(t, big.NewRat(123, 100), got.B)
	})

	t.Run("decimal reader and plain bytes writer keeps structural rules", func(t *testing.T) {
		writer := avro.MustParse(writerPlainBytes)

		type bytesRecord struct {
			A int    `avro:"a"`
			B []byte `avro:"b"`
		}
		payload, err := avro.Marshal(writer, bytesRecord{A: 9, B: []byte{0x7e}}) // 126
		require.NoError(t, err)

		schema, err := avro.NewSchemaCompatibility(avro.WithStrictLogicalTypes()).Resolve(reader, writer)
		require.NoError(t, err)

		var got decimalRecord
		require.NoError(t, avro.Unmarshal(schema, payload, &got))
		assert.Equal(t, 9, got.A)
		assert.Equal(t, big.NewRat(126, 100), got.B) // 1.26 at reader scale 2
	})
}
