package ingest

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

// Test 1: Empty entry, no nulls — verify data/recordCount/tagColumns/estimatedBytes
func TestAppendEntryToEntry_EmptyEntry(t *testing.T) {
	entry := &bufferEntry{columns: make(map[string]ColumnData)}
	batch := &bufferEntry{
		columns: map[string]ColumnData{
			"time": {Data: []int64{1, 2, 3}},
			"temp": {Data: []float64{23.5, 24.0, 25.1}},
		},
		recordCount: 3,
		tagColumns:  []string{"sensor"},
		schema:      "temp:f64,time:i64",
	}
	appendEntryToEntry(entry, batch)

	// recordCount
	if entry.recordCount != 3 {
		t.Errorf("expected recordCount=3, got %d", entry.recordCount)
	}

	// validity should remain nil (no nulls in batch)
	for _, cd := range entry.columns {
		if cd.Validity != nil {
			t.Errorf("expected validity=nil, got %v", cd.Validity)
		}
	}

	// tagColumns
	if len(entry.tagColumns) != 1 || entry.tagColumns[0] != "sensor" {
		t.Errorf("expected tagColumns=[sensor], got %v", entry.tagColumns)
	}

	// data["time"]
	times, ok := entry.columns["time"].Data.([]int64)
	if !ok {
		t.Fatal("expected []int64 for time")
	}
	if len(times) != 3 || times[0] != 1 || times[1] != 2 || times[2] != 3 {
		t.Errorf("expected time=[1,2,3], got %v", times)
	}

	// data["temp"]
	temps, ok := entry.columns["temp"].Data.([]float64)
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

// Test 2: Batch with validity — verify validity is merged into ColumnData.Validity
func TestAppendEntryToEntry_WithNulls(t *testing.T) {
	entry := &bufferEntry{columns: make(map[string]ColumnData)}
	batch := &bufferEntry{
		columns: map[string]ColumnData{
			"time": {Data: []int64{1, 2, 3}},
			"val":  {Data: []float64{10.0, 20.0, 30.0}, Validity: []bool{true, false, true}},
		},
		recordCount: 3,
	}
	appendEntryToEntry(entry, batch)

	if entry.recordCount != 3 {
		t.Errorf("expected recordCount=3, got %d", entry.recordCount)
	}

	// validity["val"] should exist and be [true, false, true]
	valid := entry.columns["val"].Validity
	if valid == nil {
		t.Fatal("expected Validity for val to be non-nil")
	}
	if len(valid) != 3 || valid[0] != true || valid[1] != false || valid[2] != true {
		t.Errorf("expected validity['val']=[true,false,true], got %v", valid)
	}
	// time should have no validity (no nulls)
	if entry.columns["time"].Validity != nil {
		t.Errorf("expected nil validity for time, got %v", entry.columns["time"].Validity)
	}

	// data columns should still be correct
	vals, ok := entry.columns["val"].Data.([]float64)
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
func TestAppendEntryToEntry_MultipleAppends(t *testing.T) {
	entry := &bufferEntry{columns: make(map[string]ColumnData)}

	batch1 := &bufferEntry{
		columns: map[string]ColumnData{
			"time": {Data: []int64{1, 2}},
			"val":  {Data: []float64{10, 20}},
		},
		recordCount: 2,
	}
	batch2 := &bufferEntry{
		columns: map[string]ColumnData{
			"time": {Data: []int64{3, 4, 5}},
			"val":  {Data: []float64{30, 40, 50}},
		},
		recordCount: 3,
	}

	appendEntryToEntry(entry, batch1)
	appendEntryToEntry(entry, batch2)

	if entry.recordCount != 5 {
		t.Errorf("expected recordCount=5, got %d", entry.recordCount)
	}

	// time should be concatenated
	times, ok := entry.columns["time"].Data.([]int64)
	if !ok {
		t.Fatal("expected []int64 for time")
	}
	if len(times) != 5 || times[0] != 1 || times[1] != 2 || times[2] != 3 || times[3] != 4 || times[4] != 5 {
		t.Errorf("expected time=[1,2,3,4,5], got %v", times)
	}

	// val should be concatenated
	vals, ok := entry.columns["val"].Data.([]float64)
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

// Test 4: Mixed nulls — first batch has nulls, second all-valid
func TestAppendEntryToEntry_MixedNulls(t *testing.T) {
	entry := &bufferEntry{columns: make(map[string]ColumnData)}

	batch1 := &bufferEntry{
		columns: map[string]ColumnData{
			"time": {Data: []int64{1, 2}},
			"val":  {Data: []float64{10, 20}, Validity: []bool{true, false}},
		},
		recordCount: 2,
	}
	batch2 := &bufferEntry{
		columns: map[string]ColumnData{
			"time": {Data: []int64{3, 4}},
			"val":  {Data: []float64{30, 40}},
		},
		recordCount: 2,
	}

	appendEntryToEntry(entry, batch1)
	appendEntryToEntry(entry, batch2)

	if entry.recordCount != 4 {
		t.Errorf("expected recordCount=4, got %d", entry.recordCount)
	}

	// validity["val"] should be [true, false, true, true] (batch2 padded with all-true)
	valid := entry.columns["val"].Validity
	if valid == nil {
		t.Fatal("expected Validity for val to be non-nil")
	}
	if len(valid) != 4 {
		t.Fatalf("expected validity['val'] with len=4, got len=%d, values=%v", len(valid), valid)
	}
	if valid[0] != true || valid[1] != false || valid[2] != true || valid[3] != true {
		t.Errorf("expected validity['val']=[true,false,true,true], got %v", valid)
	}

	// data should still be correct
	vals, ok := entry.columns["val"].Data.([]float64)
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
func TestAppendEntryToEntry_Decimal(t *testing.T) {
	val1, _ := decimal128.FromString("123.45", 10, 2)
	val2, _ := decimal128.FromString("678.90", 10, 2)

	entry := &bufferEntry{columns: make(map[string]ColumnData)}
	batch := &bufferEntry{
		columns: map[string]ColumnData{
			"time":  {Data: []int64{1, 2}},
			"price": {Data: []decimal128.Num{val1, val2}},
		},
		recordCount: 2,
	}

	appendEntryToEntry(entry, batch)

	if entry.recordCount != 2 {
		t.Errorf("expected recordCount=2, got %d", entry.recordCount)
	}

	prices, ok := entry.columns["price"].Data.([]decimal128.Num)
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
func TestAppendEntryToEntry_StringAndBool(t *testing.T) {
	entry := &bufferEntry{columns: make(map[string]ColumnData)}
	batch := &bufferEntry{
		columns: map[string]ColumnData{
			"id":     {Data: []string{"a", "b", "c"}},
			"active": {Data: []bool{true, false, true}},
		},
		recordCount: 3,
	}
	appendEntryToEntry(entry, batch)

	if entry.recordCount != 3 {
		t.Errorf("expected recordCount=3, got %d", entry.recordCount)
	}

	ids, ok := entry.columns["id"].Data.([]string)
	if !ok {
		t.Fatal("expected []string for id")
	}
	if len(ids) != 3 || ids[0] != "a" || ids[1] != "b" || ids[2] != "c" {
		t.Errorf("expected id=[a,b,c], got %v", ids)
	}

	actives, ok := entry.columns["active"].Data.([]bool)
	if !ok {
		t.Fatal("expected []bool for active")
	}
	if len(actives) != 3 || actives[0] != true || actives[1] != false || actives[2] != true {
		t.Errorf("expected active=[true,false,true], got %v", actives)
	}
}

// Test 7: Multiple batches with validity — per-column Validity in ColumnData
func TestAppendEntryToEntry_MultipleValidityAppends(t *testing.T) {
	entry := &bufferEntry{columns: make(map[string]ColumnData)}

	// Batch 1: with nulls in "val"
	batch1 := &bufferEntry{
		columns: map[string]ColumnData{
			"time": {Data: []int64{1, 2}},
			"val":  {Data: []float64{10.0, 20.0}, Validity: []bool{true, false}},
		},
		recordCount: 2,
	}
	appendEntryToEntry(entry, batch1)
	if v := entry.columns["val"].Validity; v == nil || len(v) != 2 || v[0] != true || v[1] != false {
		t.Fatalf("after batch1: expected validity[val]=[true,false], got %v", v)
	}

	// Batch 2: with nulls in "val" and "flag"
	batch2 := &bufferEntry{
		columns: map[string]ColumnData{
			"time": {Data: []int64{3, 4}},
			"val":  {Data: []float64{30.0, 40.0}, Validity: []bool{false, true}},
			"flag": {Data: []bool{true, false}, Validity: []bool{true, true}},
		},
		recordCount: 2,
	}
	appendEntryToEntry(entry, batch2)

	// val: existing (true,false) + batch2 (false,true) = [true,false,false,true]
	if v := entry.columns["val"].Validity; v == nil || len(v) != 4 || v[0] != true || v[1] != false || v[2] != false || v[3] != true {
		t.Fatalf("after batch2: expected validity[val]=[true,false,false,true], got %v", v)
	}
	// flag: new column, just batch2 = [true,true]
	if v := entry.columns["flag"].Validity; v == nil || len(v) != 2 || v[0] != true || v[1] != true {
		t.Fatalf("after batch2: expected validity[flag]=[true,true], got %v", v)
	}

	// Batch 3: nil validity — pad existing columns with all-true
	batch3 := &bufferEntry{
		columns: map[string]ColumnData{
			"time": {Data: []int64{5}},
			"val":  {Data: []float64{50.0}},
			"flag": {Data: []bool{true}},
		},
		recordCount: 1,
	}
	appendEntryToEntry(entry, batch3)

	// val: [true,false,false,true] padded with 1 true = [true,false,false,true,true]
	if v := entry.columns["val"].Validity; v == nil || len(v) != 5 || v[4] != true {
		t.Fatalf("after batch3: expected validity[val]=[true,false,false,true,true], got len=%d, %v", len(v), v)
	}
	// flag: [true,true] padded with 1 true = [true,true,true]
	if v := entry.columns["flag"].Validity; v == nil || len(v) != 3 || v[2] != true {
		t.Fatalf("after batch3: expected validity[flag]=[true,true,true], got %v", v)
	}

	if entry.recordCount != 5 {
		t.Errorf("expected recordCount=5, got %d", entry.recordCount)
	}
}
