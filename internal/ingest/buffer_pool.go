package ingest

import "sync"

// batchSlicePool reuses []*TypedColumnBatch backing arrays across flush cycles.
// Initial capacity 64 batch entries; grows as needed on first use after pool Get.
var batchSlicePool = sync.Pool{
	New: func() interface{} {
		s := make([]*TypedColumnBatch, 0, 64)
		return &s
	},
}

// GetBatchSlice returns a pooled batch slice pointer, reset to length 0.
func GetBatchSlice() *[]*TypedColumnBatch {
	s := batchSlicePool.Get().(*[]*TypedColumnBatch)
	*s = (*s)[:0]
	return s
}

// PutBatchSlice returns a batch slice to the pool.
// The caller must ensure no references remain to the slice's backing array.
func PutBatchSlice(s *[]*TypedColumnBatch) {
	// Clear pointer references to allow GC of batches
	for i := range *s {
		(*s)[i] = nil
	}
	*s = (*s)[:0]
	batchSlicePool.Put(s)
}
