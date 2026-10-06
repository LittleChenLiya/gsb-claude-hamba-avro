package avro_test

import (
	"errors"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writerThreeFieldSchema is a three-field writer schema using the old field
// name "old_name".
const writerThreeFieldSchema = `{
  "type": "record",
  "name": "User",
  "namespace": "org.hamba.avro",
  "fields": [
    {"name": "old_name", "type": "string"},
    {"name": "age", "type": "int"},
    {"name": "score", "type": "long"}
  ]
}`

// readerThreeFieldSchema renames "old_name" to "name" (via alias), promotes
// "age" from int to long, drops "score" and adds the defaulted field "email".
const readerThreeFieldSchema = `{
  "type": "record",
  "name": "User",
  "namespace": "org.hamba.avro",
  "fields": [
    {"name": "name", "type": "string", "aliases": ["old_name"]},
    {"name": "age", "type": "long"},
    {"name": "email", "type": "string", "default": "unknown"}
  ]
}`

type planWriterUser struct {
	OldName string `avro:"old_name"`
	Age     int32  `avro:"age"`
	Score   int64  `avro:"score"`
}

type planReaderUser struct {
	Name  string `avro:"name"`
	Age   int64  `avro:"age"`
	Email string `avro:"email"`
}

func TestCompilePlan_ThreeFieldAliasDefaultPromotion(t *testing.T) {
	writer := avro.MustParse(writerThreeFieldSchema)
	reader := avro.MustParse(readerThreeFieldSchema)

	// Compilation only inspects the schemas; no data is involved.
	plan, err := avro.CompilePlan(writer, reader)
	require.NoError(t, err)

	// The plan binds fields by name and alias, not by position.
	bindings := plan.Bindings()
	require.Len(t, bindings, 4)

	assert.Equal(t, avro.FieldBinding{
		WriterPath: "old_name",
		ReaderPath: "name",
		Action:     avro.FieldActionRead,
		WriterType: avro.String,
		ReaderType: avro.String,
	}, bindings[0])
	assert.Equal(t, avro.FieldBinding{
		WriterPath: "age",
		ReaderPath: "age",
		Action:     avro.FieldActionPromote,
		WriterType: avro.Int,
		ReaderType: avro.Long,
	}, bindings[1])
	assert.Equal(t, avro.FieldBinding{
		WriterPath: "score",
		Action:     avro.FieldActionSkip,
		WriterType: avro.Long,
	}, bindings[2])
	assert.Equal(t, avro.FieldBinding{
		ReaderPath: "email",
		Action:     avro.FieldActionDefault,
		ReaderType: avro.String,
	}, bindings[3])

	data, err := avro.Marshal(writer, planWriterUser{OldName: "hamba", Age: 42, Score: 99})
	require.NoError(t, err)

	var got planReaderUser
	require.NoError(t, plan.Unmarshal(data, &got))

	// The writer "old_name" value lands in the reader "name" field via the
	// alias, "age" is promoted to long, and the new "email" field is filled
	// with its default.
	assert.Equal(t, planReaderUser{Name: "hamba", Age: 42, Email: "unknown"}, got)
}

func TestCompilePlan_AliasConflictFails(t *testing.T) {
	writer := avro.MustParse(`{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "long"}
  ]
}`)
	// Two reader fields claim the writer field "id" through colliding
	// aliases.
	reader := avro.MustParse(`{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "first", "type": "long", "aliases": ["id"]},
    {"name": "second", "type": "long", "aliases": ["id"]}
  ]
}`)

	plan, err := avro.CompilePlan(writer, reader)

	require.Error(t, err)
	assert.Nil(t, plan)
	var conflict *avro.AliasConflictError
	require.True(t, errors.As(err, &conflict), "expected AliasConflictError, got %T: %v", err, err)
	assert.Equal(t, "User", conflict.Record)
	assert.Equal(t, "id", conflict.Name)
	assert.ElementsMatch(t, []string{"first", "second"}, conflict.Fields)

	// The collision is diagnosed as ambiguity, not as incompatibility.
	var incompatible *avro.IncompatibleSchemaError
	assert.False(t, errors.As(err, &incompatible))
}

func TestCompilePlan_IncompatibleTypesFail(t *testing.T) {
	writer := avro.MustParse(`{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "name", "type": "string"}
  ]
}`)
	reader := avro.MustParse(`{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "name", "type": "int"}
  ]
}`)

	plan, err := avro.CompilePlan(writer, reader)

	require.Error(t, err)
	assert.Nil(t, plan)
	var incompatible *avro.IncompatibleSchemaError
	require.True(t, errors.As(err, &incompatible), "expected IncompatibleSchemaError, got %T: %v", err, err)

	// The failure is diagnosed as incompatibility, not as ambiguity.
	var conflict *avro.AliasConflictError
	assert.False(t, errors.As(err, &conflict))
}

func TestCompilePlan_PromotionFollowsAvroRules(t *testing.T) {
	allowed := []struct{ writer, reader string }{
		{"int", "long"},
		{"int", "float"},
		{"int", "double"},
		{"long", "float"},
		{"long", "double"},
		{"float", "double"},
		{"string", "bytes"},
		{"bytes", "string"},
	}
	for _, tc := range allowed {
		t.Run("allowed_"+tc.writer+"_to_"+tc.reader, func(t *testing.T) {
			writer := avro.MustParse(promotionSchema(tc.writer))
			reader := avro.MustParse(promotionSchema(tc.reader))

			plan, err := avro.CompilePlan(writer, reader)
			require.NoError(t, err)

			require.Len(t, plan.Bindings(), 1)
			assert.Equal(t, avro.FieldActionPromote, plan.Bindings()[0].Action)
		})
	}

	disallowed := []struct{ writer, reader string }{
		{"long", "int"},
		{"double", "float"},
		{"float", "long"},
		{"int", "string"},
		{"string", "int"},
		{"boolean", "int"},
		{"double", "long"},
		{"bytes", "int"},
	}
	for _, tc := range disallowed {
		t.Run("disallowed_"+tc.writer+"_to_"+tc.reader, func(t *testing.T) {
			writer := avro.MustParse(promotionSchema(tc.writer))
			reader := avro.MustParse(promotionSchema(tc.reader))

			plan, err := avro.CompilePlan(writer, reader)

			require.Error(t, err)
			assert.Nil(t, plan)
			var incompatible *avro.IncompatibleSchemaError
			assert.True(t, errors.As(err, &incompatible), "expected IncompatibleSchemaError, got %T: %v", err, err)
		})
	}
}

func promotionSchema(typ string) string {
	return `{
  "type": "record",
  "name": "Metric",
  "fields": [
    {"name": "value", "type": "` + typ + `"}
  ]
}`
}

func TestCompilePlan_PromotionDecodesValue(t *testing.T) {
	writer := avro.MustParse(promotionSchema("int"))
	reader := avro.MustParse(promotionSchema("double"))

	plan, err := avro.CompilePlan(writer, reader)
	require.NoError(t, err)

	data, err := avro.Marshal(writer, map[string]any{"value": int32(7)})
	require.NoError(t, err)

	var got struct {
		Value float64 `avro:"value"`
	}
	require.NoError(t, plan.Unmarshal(data, &got))
	assert.InDelta(t, 7.0, got.Value, 0.0)
}

func TestCompilePlan_ReaderSchemaModificationDoesNotAffectPlan(t *testing.T) {
	writer := avro.MustParse(writerThreeFieldSchema)
	reader := avro.MustParse(readerThreeFieldSchema)

	plan, err := avro.CompilePlan(writer, reader)
	require.NoError(t, err)

	// Mutate the reader schema after the plan was compiled: replace its
	// first field with an incompatible one. The compiled plan holds its own
	// resolved snapshot and must not observe the change.
	rec := reader.(*avro.RecordSchema)
	bogus, err := avro.NewField("bogus", avro.NewPrimitiveSchema(avro.Boolean, nil))
	require.NoError(t, err)
	rec.Fields()[0] = bogus

	data, err := avro.Marshal(writer, planWriterUser{OldName: "hamba", Age: 42, Score: 99})
	require.NoError(t, err)

	var got planReaderUser
	require.NoError(t, plan.Unmarshal(data, &got))
	assert.Equal(t, planReaderUser{Name: "hamba", Age: 42, Email: "unknown"}, got)
}

func TestCompilePlan_RequiresRecordSchemas(t *testing.T) {
	primitive := avro.NewPrimitiveSchema(avro.String, nil)
	record := avro.MustParse(writerThreeFieldSchema)

	_, err := avro.CompilePlan(primitive, record)
	var incompatible *avro.IncompatibleSchemaError
	assert.True(t, errors.As(err, &incompatible))

	_, err = avro.CompilePlan(record, primitive)
	assert.True(t, errors.As(err, &incompatible))

	_, err = avro.CompilePlan(nil, record)
	assert.True(t, errors.As(err, &incompatible))
}

func TestCompilePlan_OriginalUnmarshalPreserved(t *testing.T) {
	// The original Unmarshal API keeps working untouched.
	schema := avro.MustParse(writerThreeFieldSchema)

	data, err := avro.Marshal(schema, planWriterUser{OldName: "hamba", Age: 42, Score: 99})
	require.NoError(t, err)

	var got planWriterUser
	require.NoError(t, avro.Unmarshal(schema, data, &got))
	assert.Equal(t, planWriterUser{OldName: "hamba", Age: 42, Score: 99}, got)
}
