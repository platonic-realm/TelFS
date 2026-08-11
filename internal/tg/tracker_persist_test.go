package tg

import (
	"errors"
	"testing"
)

// Marshal → Seed into a fresh tracker → totals continue from the
// baseline; a second marshal carries the sum, so plain overwrites of
// the stored value never lose history.
func TestTrackerStatePersistRoundTrip(t *testing.T) {
	a := NewTracker(8)
	id := a.Begin("UploadSaveBigFilePartRequest", 1000)
	a.End(id, nil, 0, 0)
	id = a.Begin("UploadGetFileRequest", 64)
	a.End(id, nil, 0, 4096)
	id = a.Begin("MessagesSendMediaRequest", 200)
	a.End(id, errors.New("boom"), 0, 0)

	blob, err := a.MarshalState()
	if err != nil {
		t.Fatal(err)
	}

	b := NewTracker(8)
	if err := b.SeedState(blob); err != nil {
		t.Fatal(err)
	}
	// New traffic on the seeded tracker.
	id = b.Begin("UploadSaveBigFilePartRequest", 500)
	b.End(id, nil, 0, 0)

	snap := b.Snapshot()
	if snap.TotalOK != 3 { // 2 seeded + 1 live
		t.Fatalf("TotalOK = %d, want 3", snap.TotalOK)
	}
	if snap.TotalErr != 1 {
		t.Fatalf("TotalErr = %d, want 1", snap.TotalErr)
	}
	if snap.TotalBytesTx != 1000+64+200+500 {
		t.Fatalf("TotalBytesTx = %d", snap.TotalBytesTx)
	}
	if snap.TotalBytesRx != 4096 {
		t.Fatalf("TotalBytesRx = %d", snap.TotalBytesRx)
	}
	var save MethodCount
	for _, mc := range snap.PerMethod {
		if mc.Method == "UploadSaveBigFilePartRequest" {
			save = mc
		}
	}
	if save.OK != 2 || save.Bytes != 1500 {
		t.Fatalf("seeded+live method merge wrong: %+v", save)
	}

	// Second marshal reflects the SUM — overwrite-safe.
	blob2, err := b.MarshalState()
	if err != nil {
		t.Fatal(err)
	}
	c := NewTracker(8)
	if err := c.SeedState(blob2); err != nil {
		t.Fatal(err)
	}
	if got := c.Snapshot().TotalOK; got != 3 {
		t.Fatalf("re-seeded TotalOK = %d, want 3", got)
	}
}

func TestTrackerSeedStateRejectsGarbage(t *testing.T) {
	tr := NewTracker(4)
	if err := tr.SeedState([]byte("not json")); err == nil {
		t.Fatal("SeedState accepted garbage")
	}
	if got := tr.Snapshot().TotalOK; got != 0 {
		t.Fatalf("garbage seed mutated counters: %d", got)
	}
}
