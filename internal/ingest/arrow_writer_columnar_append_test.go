package ingest

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

// Test 1: Empty entry, no nulls — verify data/recordCount/tagColumns/estimatedBytes
func TestAppendTypedBatchToEntry_EmptyEntry(t *testing.T) {
	entry := &bufferEntry{data: make(map[string]interface{}), validity: nil}
	batch := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time": []int64{1, 2, 3},
			"temp": []float64{23.5, 24.0, 25.1},
		},
		Validity:   nil,
		TagColumns: []string{"sensor"},
		Signature:  "temp:f64,time:i64",
	}
	appendTypedBatchToEntry(entry, batch, 3)

	// recordCount
	if entry.recordCount != 3 {
		t.Errorf("expected recordCount=3, got %d", entry.recordCount)
	}

	// validity should remain nil (no nulls in batch)
	if entry.validity != nil {
		t.Errorf("expected validity=nil, got %v", entry.validity)
	}

	// tagColumns
	if len(entry.tagColumns) != 1 || entry.tagColumns[0] != "sensor" {
		t.Errorf("expected tagColumns=[sensor], got %v", entry.tagColumns)
	}

	// data["time"]
	times, ok := entry.data["time"].([]int64)
	if !ok {
		t.Fatal("expected []int64 for time")
	}
	if len(times) != 3 || times[0] != 1 || times[1] != 2 || times[2] != 3 {
		t.Errorf("expected time=[1,2,3], got %v", times)
	}

	// data["temp"]
	temps, ok := entry.data["temp"].([]float64)
	if !ok {
		t.Fatal("expected []float64 for temp")
	}
	if len(temps) != 3 || temps[0] != 23.5 || temps[1] != 24.0 || temps[2] != 25.1 {
		t.Errorf("expected temp=[23.5,24.0,25.1], got %v", temps)
	}

	// estimatedBytes: 3 rows * (8+8+2) = 54
	if entry.estimatedBytes != 54 {
		t.Errorf("expected estimatedBytes=54, got %d", entry.estimatedBytes)
	}
}

// Test 2: Batch with validity map — verify validity is merged
func TestAppendTypedBatchToEntry_WithNulls(t *testing.T) {
	entry := &bufferEntry{data: make(map[string]interface{}), validity: nil}
	batch := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time": []int64{1, 2, 3},
			"val":  []float64{10.0, 20.0, 30.0},
		},
		Validity: map[string][]bool{
			"val": {true, false, true},
		},
	}
	appendTypedBatchToEntry(entry, batch, 3)

	if entry.recordCount != 3 {
		t.Errorf("expected recordCount=3, got %d", entry.recordCount)
	}

	// validity should be initialized
	if entry.validity == nil {
		t.Fatal("expected validity to be non-nil")
	}

	// validity["val"]
	valid, ok := entry.validity["val"]
	if !ok {
		t.Fatal("expected validity['val'] to exist")
	}
	if len(valid) != 3 || valid[0] != true || valid[1] != false || valid[2] != true {
		t.Errorf("expected validity['val']=[true,false,true], got %v", valid)
	}

	// data columns should still be correct
	vals, ok := entry.data["val"].([]float64)
	if !ok {
		t.Fatal("expected []float64 for val")
	}
	if len(vals) != 3 || vals[0] != 10.0 || vals[1] != 20.0 || vals[2] != 30.0 {
		t.Errorf("expected val=[10,20,30], got %v", vals)
	}

	// estimatedBytes: 3 rows * (8+8+2) = 54
	if entry.estimatedBytes != 54 {
		t.Errorf("expected estimatedBytes=54, got %d", entry.estimatedBytes)
	}
}

// Test 3: Multiple appends — verify concatenation order
func TestAppendTypedBatchToEntry_MultipleAppends(t *testing.T) {
	entry := &bufferEntry{data: make(map[string]interface{}), validity: nil}

	batch1 := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time": []int64{1, 2},
			"val":  []float64{10, 20},
		},
		Validity: nil,
	}
	batch2 := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time": []int64{3, 4, 5},
			"val":  []float64{30, 40, 50},
		},
		Validity: nil,
	}

	appendTypedBatchToEntry(entry, batch1, 2)
	appendTypedBatchToEntry(entry, batch2, 3)

	if entry.recordCount != 5 {
		t.Errorf("expected recordCount=5, got %d", entry.recordCount)
	}

	// time should be concatenated
	times, ok := entry.data["time"].([]int64)
	if !ok {
		t.Fatal("expected []int64 for time")
	}
	if len(times) != 5 || times[0] != 1 || times[1] != 2 || times[2] != 3 || times[3] != 4 || times[4] != 5 {
		t.Errorf("expected time=[1,2,3,4,5], got %v", times)
	}

	// val should be concatenated
	vals, ok := entry.data["val"].([]float64)
	if !ok {
		t.Fatal("expected []float64 for val")
	}
	if len(vals) != 5 || vals[0] != 10 || vals[1] != 20 || vals[2] != 30 || vals[3] != 40 || vals[4] != 50 {
		t.Errorf("expected val=[10,20,30,40,50], got %v", vals)
	}

	// estimatedBytes: batch1 2*18=36 + batch2 3*18=54 = 90
	if entry.estimatedBytes != 90 {
		t.Errorf("expected estimatedBytes=90, got %d", entry.estimatedBytes)
	}
}

// Test 4: Mixed nulls — first batch has nulls, second doesn't (must pad with true)
func TestAppendTypedBatchToEntry_MixedNulls(t *testing.T) {
	entry := &bufferEntry{data: make(map[string]interface{}), validity: nil}

	batch1 := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time": []int64{1, 2},
			"val":  []float64{10, 20},
		},
		Validity: map[string][]bool{
			"val": {true, false},
		},
	}
	batch2 := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time": []int64{3, 4},
			"val":  []float64{30, 40},
		},
		Validity: nil, // all valid
	}

	appendTypedBatchToEntry(entry, batch1, 2)
	appendTypedBatchToEntry(entry, batch2, 2)

	if entry.recordCount != 4 {
		t.Errorf("expected recordCount=4, got %d", entry.recordCount)
	}

	// validity should be non-nil
	if entry.validity == nil {
		t.Fatal("expected validity to be non-nil")
	}

	// validity["val"] should be [true, false, true, true] (batch2 padded with all-true)
	valid, ok := entry.validity["val"]
	if !ok {
		t.Fatal("expected validity['val'] to exist")
	}
	if len(valid) != 4 {
		t.Fatalf("expected validity['val'] with len=4, got len=%d, values=%v", len(valid), valid)
	}
	if valid[0] != true || valid[1] != false || valid[2] != true || valid[3] != true {
		t.Errorf("expected validity['val']=[true,false,true,true], got %v", valid)
	}

	// data should still be correct
	vals, ok := entry.data["val"].([]float64)
	if !ok {
		t.Fatal("expected []float64 for val")
	}
	if len(vals) != 4 || vals[0] != 10 || vals[1] != 20 || vals[2] != 30 || vals[3] != 40 {
		t.Errorf("expected val=[10,20,30,40], got %v", vals)
	}

	// estimatedBytes: batch1 2*18=36 + batch2 2*18=36 = 72
	if entry.estimatedBytes != 72 {
		t.Errorf("expected estimatedBytes=72, got %d", entry.estimatedBytes)
	}
}

// Test 5: Decimal128 column type — verify decimal128.Num columns are appended
func TestAppendTypedBatchToEntry_Decimal(t *testing.T) {
	val1, _ := decimal128.FromString("123.45", 10, 2)
	val2, _ := decimal128.FromString("678.90", 10, 2)

	entry := &bufferEntry{data: make(map[string]interface{}), validity: nil}
	batch := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time":  []int64{1, 2},
			"price": []decimal128.Num{val1, val2},
		},
		Validity: nil,
	}

	appendTypedBatchToEntry(entry, batch, 2)

	if entry.recordCount != 2 {
		t.Errorf("expected recordCount=2, got %d", entry.recordCount)
	}

	prices, ok := entry.data["price"].([]decimal128.Num)
	if !ok {
		t.Fatal("expected []decimal128.Num for price")
	}
	if len(prices) != 2 {
		t.Fatalf("expected prices len=2, got %d", len(prices))
	}
	if prices[0] != val1 || prices[1] != val2 {
		t.Errorf("expected prices=%v, got %v", []decimal128.Num{val1, val2}, prices)
	}
}

// Test 6: String and Bool column types
func TestAppendTypedBatchToEntry_StringAndBool(t *testing.T) {
	entry := &bufferEntry{data: make(map[string]interface{}), validity: nil}
	batch := &TypedColumnBatch{
		Data: map[string]interface{}{
			"id":     []string{"a", "b", "c"},
			"active": []bool{true, false, true},
		},
		Validity: nil,
	}
	appendTypedBatchToEntry(entry, batch, 3)

	if entry.recordCount != 3 {
		t.Errorf("expected recordCount=3, got %d", entry.recordCount)
	}

	ids, ok := entry.data["id"].([]string)
	if !ok {
		t.Fatal("expected []string for id")
	}
	if len(ids) != 3 || ids[0] != "a" || ids[1] != "b" || ids[2] != "c" {
		t.Errorf("expected id=[a,b,c], got %v", ids)
	}

	actives, ok := entry.data["active"].([]bool)
	if !ok {
		t.Fatal("expected []bool for active")
	}
	if len(actives) != 3 || actives[0] != true || actives[1] != false || actives[2] != true {
		t.Errorf("expected active=[true,false,true], got %v", actives)
	}
}

// Test 7: First batch has validity, creates new validity map
// Second batch has additional validity columns that don't exist in entry yet
// Third batch has nil validity — pads existing columns
func TestAppendTypedBatchToEntry_MultipleValidityAppends(t *testing.T) {
	entry := &bufferEntry{data: make(map[string]interface{}), validity: nil}

	// Batch 1: with nulls in "val"
	batch1 := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time": []int64{1, 2},
			"val":  []float64{10.0, 20.0},
		},
		Validity: map[string][]bool{
			"val": {true, false},
		},
	}
	appendTypedBatchToEntry(entry, batch1, 2)
	if v := entry.validity["val"]; len(v) != 2 || v[0] != true || v[1] != false {
		t.Fatalf("after batch1: expected validity[val]=[true,false], got %v", v)
	}

	// Batch 2: with nulls in "val" and "flag"
	batch2 := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time": []int64{3, 4},
			"val":  []float64{30.0, 40.0},
			"flag": []bool{true, false},
		},
		Validity: map[string][]bool{
			"val":  {false, true},
			"flag": {true, true},
		},
	}
	appendTypedBatchToEntry(entry, batch2, 2)

	// val: existing (true,false) + batch2 (false,true) = [true,false,false,true]
	if v := entry.validity["val"]; len(v) != 4 || v[0] != true || v[1] != false || v[2] != false || v[3] != true {
		t.Fatalf("after batch2: expected validity[val]=[true,false,false,true], got %v", v)
	}
	// flag: new column, just batch2 = [true,true]
	if v := entry.validity["flag"]; len(v) != 2 || v[0] != true || v[1] != true {
		t.Fatalf("after batch2: expected validity[flag]=[true,true], got %v", v)
	}

	// Batch 3: nil validity — pad existing columns with all-true
	batch3 := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time": []int64{5},
			"val":  []float64{50.0},
			"flag": []bool{true},
		},
		Validity: nil,
	}
	appendTypedBatchToEntry(entry, batch3, 1)

	// val: [true,false,false,true] padded with 1 true = [true,false,false,true,true]
	if v := entry.validity["val"]; len(v) != 5 || v[4] != true {
		t.Fatalf("after batch3: expected validity[val]=[true,false,false,true,true], got len=%d, %v", len(v), v)
	}
	// flag: [true,true] padded with 1 true = [true,true,true]
	if v := entry.validity["flag"]; len(v) != 3 || v[2] != true {
		t.Fatalf("after batch3: expected validity[flag]=[true,true,true], got %v", v)
	}

	if entry.recordCount != 5 {
		t.Errorf("expected recordCount=5, got %d", entry.recordCount)
	}
}
