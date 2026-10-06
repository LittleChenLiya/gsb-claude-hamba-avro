package avro_test

import (
	"errors"
	"testing"

	"github.com/hamba/avro/v2"
)

// eventWriter is the writer-side struct used to produce Avro data.
type eventWriter struct {
	OldName string  `avro:"old_name"`
	Count   int32   `avro:"count"`
	Score   float32 `avro:"score"`
}

// eventReader is the reader-side struct populated through the plan.
type eventReader struct {
	NewName string  `avro:"new_name"`
	Count   int64   `avro:"count"`
	Score   float64 `avro:"score"`
	Tag     string  `avro:"tag"`
}

const writerSchemaThreeFields = `{
	"type": "record",
	"name": "Event",
	"fields": [
		{"name": "old_name", "type": "string"},
		{"name": "count", "type": "int"},
		{"name": "score", "type": "float"}
	]
}`

const readerSchemaThreeFields = `{
	"type": "record",
	"name": "Event",
	"fields": [
		{"name": "new_name", "type": "string", "aliases": ["old_name"]},
		{"name": "count", "type": "long"},
		{"name": "score", "type": "double"},
		{"name": "tag", "type": "string", "default": "default-tag"}
	]
}`

// TestCompilePlanAliasMappingAndPromotion is the three-field schema example.
// It verifies that a writer field is bound to a reader field through a reader
// alias, that numeric promotions follow the Avro rules, and that a new reader
// field is filled with its default.
func TestCompilePlanAliasMappingAndPromotion(t *testing.T) {
	writer := avro.MustParse(writerSchemaThreeFields)
	reader := avro.MustParse(readerSchemaThreeFields)

	plan, err := avro.CompilePlan(writer, reader)
	if err != nil {
		t.Fatalf("CompilePlan() error = %v", err)
	}

	// The plan must expose the explicit bindings it compiled.
	bindings := plan.Bindings()
	if len(bindings) != 4 {
		t.Fatalf("Bindings() len = %d, want 4", len(bindings))
	}

	wantActions := []avro.BindingAction{
		avro.BindingBind, // old_name -> new_name (alias)
		avro.BindingBind, // count -> count (int -> long)
		avro.BindingBind, // score -> score (float -> double)
		avro.BindingDefault, // tag -> default
	}
	for i, want := range wantActions {
		if bindings[i].Action != want {
			t.Errorf("Bindings()[%d].Action = %s, want %s", i, bindings[i].Action, want)
		}
	}

	// The first binding must map the writer field old_name to the reader field
	// new_name through the alias.
	if bindings[0].WriterField.Name() != "old_name" {
		t.Errorf("Bindings()[0].WriterField.Name() = %q, want old_name", bindings[0].WriterField.Name())
	}
	if bindings[0].ReaderField.Name() != "new_name" {
		t.Errorf("Bindings()[0].ReaderField.Name() = %q, want new_name", bindings[0].ReaderField.Name())
	}

	// Execute the plan against data written with the writer schema.
	in := eventWriter{OldName: "hello", Count: 42, Score: 3.14}
	data, err := avro.Marshal(writer, in)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	var out eventReader
	if err := plan.Unmarshal(data, &out); err != nil {
		t.Fatalf("plan.Unmarshal() error = %v", err)
	}

	if out.NewName != "hello" {
		t.Errorf("NewName = %q, want hello", out.NewName)
	}
	if out.Count != 42 {
		t.Errorf("Count = %d, want 42", out.Count)
	}
	if out.Tag != "default-tag" {
		t.Errorf("Tag = %q, want default-tag", out.Tag)
	}
}

// TestCompilePlanAliasCollision verifies that two reader fields claiming the
// same alias cause compilation to fail with an AmbiguityError.
func TestCompilePlanAliasCollision(t *testing.T) {
	writer := avro.MustParse(`{
		"type": "record",
		"name": "Event",
		"fields": [
			{"name": "old_name", "type": "string"}
		]
	}`)
	reader := avro.MustParse(`{
		"type": "record",
		"name": "Event",
		"fields": [
			{"name": "new_name", "type": "string", "aliases": ["old_name"]},
			{"name": "other_name", "type": "string", "aliases": ["old_name"]}
		]
	}`)

	_, err := avro.CompilePlan(writer, reader)
	if err == nil {
		t.Fatal("CompilePlan() error = nil, want AmbiguityError")
	}

	var amb *avro.AmbiguityError
	if !errors.As(err, &amb) {
		t.Fatalf("CompilePlan() error = %T, want *AmbiguityError", err)
	}
}

// TestCompilePlanReaderFieldBoundTwice verifies that a reader field matching
// two writer fields causes compilation to fail with an AmbiguityError.
func TestCompilePlanReaderFieldBoundTwice(t *testing.T) {
	writer := avro.MustParse(`{
		"type": "record",
		"name": "Event",
		"fields": [
			{"name": "old_name", "type": "string"},
			{"name": "old_alias", "type": "string"}
		]
	}`)
	reader := avro.MustParse(`{
		"type": "record",
		"name": "Event",
		"fields": [
			{"name": "new_name", "type": "string", "aliases": ["old_name", "old_alias"]}
		]
	}`)

	_, err := avro.CompilePlan(writer, reader)
	if err == nil {
		t.Fatal("CompilePlan() error = nil, want AmbiguityError")
	}

	var amb *avro.AmbiguityError
	if !errors.As(err, &amb) {
		t.Fatalf("CompilePlan() error = %T, want *AmbiguityError", err)
	}
}

// TestCompilePlanIncompatibleType verifies that a type mismatch with no Avro
// promotion (e.g. string to int) fails with an IncompatibilityError, not an
// AmbiguityError.
func TestCompilePlanIncompatibleType(t *testing.T) {
	writer := avro.MustParse(`{
		"type": "record",
		"name": "Event",
		"fields": [
			{"name": "count", "type": "string"}
		]
	}`)
	reader := avro.MustParse(`{
		"type": "record",
		"name": "Event",
		"fields": [
			{"name": "count", "type": "int"}
		]
	}`)

	_, err := avro.CompilePlan(writer, reader)
	if err == nil {
		t.Fatal("CompilePlan() error = nil, want IncompatibilityError")
	}

	var inc *avro.IncompatibilityError
	if !errors.As(err, &inc) {
		t.Fatalf("CompilePlan() error = %T, want *IncompatibilityError", err)
	}
}

// TestCompilePlanMissingRequiredField verifies that a reader field missing
// from the writer with no default fails with an IncompatibilityError.
func TestCompilePlanMissingRequiredField(t *testing.T) {
	writer := avro.MustParse(`{
		"type": "record",
		"name": "Event",
		"fields": [
			{"name": "old_name", "type": "string"}
		]
	}`)
	reader := avro.MustParse(`{
		"type": "record",
		"name": "Event",
		"fields": [
			{"name": "old_name", "type": "string"},
			{"name": "required", "type": "string"}
		]
	}`)

	_, err := avro.CompilePlan(writer, reader)
	if err == nil {
		t.Fatal("CompilePlan() error = nil, want IncompatibilityError")
	}

	var inc *avro.IncompatibilityError
	if !errors.As(err, &inc) {
		t.Fatalf("CompilePlan() error = %T, want *IncompatibilityError", err)
	}
}

// TestCompilePlanPromotions verifies that the allowed Avro promotions are
// accepted and that disallowed promotions are rejected.
func TestCompilePlanPromotions(t *testing.T) {
	tests := []struct {
		name     string
		writer   string
		reader   string
		wantErr  bool
	}{
		{name: "int to long", writer: "int", reader: "long"},
		{name: "int to float", writer: "int", reader: "float"},
		{name: "int to double", writer: "int", reader: "double"},
		{name: "long to float", writer: "long", reader: "float"},
		{name: "long to double", writer: "long", reader: "double"},
		{name: "float to double", writer: "float", reader: "double"},
		{name: "string to bytes", writer: "string", reader: "bytes"},
		{name: "bytes to string", writer: "bytes", reader: "string"},
		{name: "int to string rejected", writer: "int", reader: "string", wantErr: true},
		{name: "long to int rejected", writer: "long", reader: "int", wantErr: true},
		{name: "double to float rejected", writer: "double", reader: "float", wantErr: true},
		{name: "string to int rejected", writer: "string", reader: "int", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer := avro.MustParse(`{"type": "record", "name": "R", "fields": [{"name": "f", "type": "` + tt.writer + `"}]}`)
			reader := avro.MustParse(`{"type": "record", "name": "R", "fields": [{"name": "f", "type": "` + tt.reader + `"}]}`)

			_, err := avro.CompilePlan(writer, reader)
			if tt.wantErr && err == nil {
				t.Fatal("CompilePlan() error = nil, want error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("CompilePlan() error = %v, want nil", err)
			}
		})
	}
}

// TestCompilePlanDoesNotReadData verifies that compiling a plan does not
// require any data. It simply checks that CompilePlan succeeds with no data
// argument.
func TestCompilePlanDoesNotReadData(t *testing.T) {
	writer := avro.MustParse(writerSchemaThreeFields)
	reader := avro.MustParse(readerSchemaThreeFields)

	plan, err := avro.CompilePlan(writer, reader)
	if err != nil {
		t.Fatalf("CompilePlan() error = %v", err)
	}
	if plan == nil {
		t.Fatal("CompilePlan() = nil, want non-nil plan")
	}
}

// TestPlanReaderMutationIsolation verifies that a plan compiled from a reader
// schema is not affected by later modification of that reader schema.
func TestPlanReaderMutationIsolation(t *testing.T) {
	oldField, err := avro.NewField("old_name", avro.MustParse("string"))
	if err != nil {
		t.Fatalf("NewField() error = %v", err)
	}
	fields := []*avro.Field{oldField}
	reader, err := avro.NewRecordSchema("Event", "", fields)
	if err != nil {
		t.Fatalf("NewRecordSchema() error = %v", err)
	}

	writer := avro.MustParse(`{
		"type": "record",
		"name": "Event",
		"fields": [
			{"name": "old_name", "type": "string"}
		]
	}`)

	plan, err := avro.CompilePlan(writer, reader)
	if err != nil {
		t.Fatalf("CompilePlan() error = %v", err)
	}

	// Mutate the reader schema after the plan was compiled by replacing the
	// field in the underlying slice. The plan must keep its snapshot.
	mutatedField, err := avro.NewField("mutated", avro.MustParse("int"))
	if err != nil {
		t.Fatalf("NewField() error = %v", err)
	}
	fields[0] = mutatedField

	in := eventWriter{OldName: "hello", Count: 1, Score: 2.0}
	data, err := avro.Marshal(writer, in)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	var out struct {
		OldName string `avro:"old_name"`
	}
	if err := plan.Unmarshal(data, &out); err != nil {
		t.Fatalf("plan.Unmarshal() error = %v", err)
	}
	if out.OldName != "hello" {
		t.Errorf("OldName = %q, want hello", out.OldName)
	}
}
