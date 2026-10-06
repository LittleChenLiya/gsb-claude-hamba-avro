package avro_test

import (
	"fmt"
	"log"

	"github.com/hamba/avro/v2"
)

// ExampleCompilePlan demonstrates compiling an explicit writer-to-reader plan
// and executing it against data written with the writer schema.
//
// The writer schema uses the field name old_name, while the reader schema uses
// new_name with an alias back to old_name. The reader also promotes int to
// long and float to double, and fills a new field tag with its default.
func ExampleCompilePlan() {
	writerSchema := avro.MustParse(`{
		"type": "record",
		"name": "Event",
		"fields": [
			{"name": "old_name", "type": "string"},
			{"name": "count", "type": "int"},
			{"name": "score", "type": "float"}
		]
	}`)

	readerSchema := avro.MustParse(`{
		"type": "record",
		"name": "Event",
		"fields": [
			{"name": "new_name", "type": "string", "aliases": ["old_name"]},
			{"name": "count", "type": "long"},
			{"name": "score", "type": "double"},
			{"name": "tag", "type": "string", "default": "default-tag"}
		]
	}`)

	plan, err := avro.CompilePlan(writerSchema, readerSchema)
	if err != nil {
		log.Fatal(err)
	}

	type WriterEvent struct {
		OldName string  `avro:"old_name"`
		Count   int32   `avro:"count"`
		Score   float32 `avro:"score"`
	}

	data, err := avro.Marshal(writerSchema, WriterEvent{OldName: "hello", Count: 42, Score: 3.14})
	if err != nil {
		log.Fatal(err)
	}

	type ReaderEvent struct {
		NewName string  `avro:"new_name"`
		Count   int64   `avro:"count"`
		Score   float64 `avro:"score"`
		Tag     string  `avro:"tag"`
	}

	var out ReaderEvent
	if err := plan.Unmarshal(data, &out); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("new_name=%s count=%d tag=%s\n", out.NewName, out.Count, out.Tag)

	// Output: new_name=hello count=42 tag=default-tag
}
