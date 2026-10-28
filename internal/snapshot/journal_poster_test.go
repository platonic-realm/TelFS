package snapshot

import (
	"fmt"
	"testing"

	"telfs/internal/meta"
)

func TestJournalBatches(t *testing.T) {
	mk := func(sizes ...int) []meta.JournalEntry {
		out := make([]meta.JournalEntry, len(sizes))
		for i, n := range sizes {
			out[i] = meta.JournalEntry{Seq: int64(i + 1), OpJSON: make([]byte, n)}
		}
		return out
	}
	for _, tc := range []struct {
		name         string
		sizes        []int
		maxOps, maxB int
		want         string
	}{
		{"by count (format 1)", []int{1, 1, 1, 1, 1}, 2, 0, "[[0 2] [2 4] [4 5]]"},
		{"by bytes (format 2)", []int{40, 40, 40, 40, 40}, 0, 100, "[[0 2] [2 4] [4 5]]"},
		{"exact fit", []int{50, 50, 50}, 0, 100, "[[0 2] [2 3]]"},
		{"oversized op alone", []int{10, 300, 10}, 0, 100, "[[0 1] [1 2] [2 3]]"},
		{"both limits", []int{1, 1, 1, 90, 20}, 3, 100, "[[0 3] [3 4] [4 5]]"},
		{"empty", nil, 0, 100, "[]"},
	} {
		got := fmt.Sprint(journalBatches(mk(tc.sizes...), tc.maxOps, tc.maxB))
		if got != tc.want {
			t.Errorf("%s: got %s, want %s", tc.name, got, tc.want)
		}
	}
}
