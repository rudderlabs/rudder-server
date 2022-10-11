package dsindex

import (
	"fmt"
	"strconv"
	"strings"
)

// Index represents a sortable dataset index, e.g. 1 < 1_1 < 1_1_1 < 1_2 < 2
type Index struct {
	segments []int
}

// MustBump returns the next index that is greater than the current one,
// but still less than the index provided in the parameter. Panics on error
func (idx *Index) MustBump(previous *Index) *Index {
	res, err := idx.Bump(previous)
	if err != nil {
		panic(err)
	}
	return res
}

// Bump returns the next index that is greater than the current one,
// but still less than the index provided in the parameter.
//
// Bump doesn't increment the first, major segment of the index, it starts from the second segment instead.
func (idx *Index) Bump(before *Index) (*Index, error) {
	if !idx.Less(before) {
		return nil, fmt.Errorf("%s is not before %s", idx, before)
	}
	var segment int // never increasing the major segment (index 0)
	for {
		segment++
		res, err := idx.Increment(segment)
		if err != nil {
			return nil, err
		}
		pl := idx.Less(res)
		rl := res.Less(before)
		if pl && rl {
			return res, nil
		}
	}
}

// MustIncrement returns a new dataset index that is incremented by one in the specified segment, panics on error
func (idx *Index) MustIncrement(segment int) *Index {
	res, err := idx.Increment(segment)
	if err != nil {
		panic(err)
	}
	return res
}

// Increment returns a new dataset index that is incremented by one in the specified segment
func (idx *Index) Increment(segment int) (*Index, error) {
	if segment < 0 || segment > idx.Length() {
		return nil, fmt.Errorf("cannot increment segment %d of %s", segment, idx)
	}
	var res *Index = &Index{}
	for i := 0; i < segment; i++ {
		toAppend := idx.segments[i]
		res.segments = append(res.segments, toAppend)
	}
	var lastSegment int
	if idx.Length() > segment {
		lastSegment = idx.segments[segment]
	}
	res.segments = append(res.segments, lastSegment+1)
	return res, nil
}

// Less returns true if this dataset index is Less than the other dataset index
func (idx *Index) Less(other *Index) bool {
	for i, segment := range idx.segments {
		if i >= other.Length() {
			return false
		}
		if segment < other.segments[i] {
			return true
		}
		if segment > other.segments[i] {
			return false
		}
	}
	return other.Length() > idx.Length()
}

// Length returns the number of segments in the dataset index
func (idx *Index) Length() int {
	return len(idx.segments)
}

// String returns a string representation of the dataset index
func (idx *Index) String() string {
	var result []string
	for _, segment := range idx.segments {
		result = append(result, strconv.Itoa(segment))
	}
	return strings.Join(result, "_")
}

// MustParse returns a dataset index from a string representation, panics on error
func MustParse(value string) *Index {
	res, err := Parse(value)
	if err != nil {
		panic(err)
	}
	return res
}

// Parse returns a dataset index from a string representation
func Parse(value string) (*Index, error) {
	var result Index
	stringSegments := strings.Split(value, "_")
	for i, stringSegment := range stringSegments {
		segment, err := strconv.Atoi(stringSegment)
		if err != nil {
			return nil, fmt.Errorf("illegal value for segment %d: %s", i+1, value)
		}
		if i > 0 && segment <= 0 {
			return nil, fmt.Errorf("value for segment %d cannot be less than or equal to zero: %s", i+1, value)
		}
		result.segments = append(result.segments, segment)
	}
	return &result, nil
}
