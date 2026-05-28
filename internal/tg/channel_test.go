package tg

import "testing"

func TestNormalizeChannelID(t *testing.T) {
	cases := []struct {
		name    string
		in      int64
		want    int64
		wantErr bool
	}{
		{"raw positive", 1234567890, 1234567890, false},
		{"marked form", -1001234567890, 1234567890, false},
		{"marked form short", -1001, 1, false},
		{"basic-chat id (small negative)", -12345, 0, true},
		{"zero", 0, 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := NormalizeChannelID(c.in)
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %d", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Fatalf("got %d, want %d", got, c.want)
			}
		})
	}
}
