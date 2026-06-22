package flight

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// mergedReader concatenates multiple RecordReaders with identical schemas
// into a single sequential stream. Each reader is drained completely before
// moving to the next. The merged schema is taken from the first reader.
type mergedReader struct {
	readers []array.RecordReader
	current int
	schema  *arrow.Schema
}

// NewMergedReader creates a reader that concatenates the given readers in order.
// All readers must share the same schema. Returns error if the readers slice is empty.
func NewMergedReader(readers []array.RecordReader) (array.RecordReader, error) {
	if len(readers) == 0 {
		return nil, fmt.Errorf("merged reader: at least one reader required")
	}
	// Filter nil readers (from failed shards)
	valid := make([]array.RecordReader, 0, len(readers))
	for _, r := range readers {
		if r != nil {
			valid = append(valid, r)
		}
	}
	if len(valid) == 0 {
		return nil, fmt.Errorf("merged reader: all readers are nil")
	}
	return &mergedReader{
		readers: valid,
		schema:  valid[0].Schema(),
	}, nil
}

func (m *mergedReader) Retain() {
	for _, r := range m.readers {
		r.Retain()
	}
}

func (m *mergedReader) Schema() *arrow.Schema { return m.schema }

func (m *mergedReader) Next() bool {
	for m.current < len(m.readers) {
		if m.readers[m.current].Next() {
			return true
		}
		if err := m.readers[m.current].Err(); err != nil {
			return false
		}
		m.current++
	}
	return false
}

func (m *mergedReader) RecordBatch() arrow.RecordBatch {
	return m.Record()
}

func (m *mergedReader) Record() arrow.RecordBatch {
	if m.current < len(m.readers) {
		return m.readers[m.current].RecordBatch()
	}
	return nil
}

func (m *mergedReader) Err() error {
	if m.current < len(m.readers) {
		return m.readers[m.current].Err()
	}
	return nil
}

func (m *mergedReader) Release() {
	for _, r := range m.readers {
		r.Release()
	}
	m.readers = nil
}
