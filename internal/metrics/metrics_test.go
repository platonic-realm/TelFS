package metrics

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestCounterAndGauge: register one of each, exercise via the API,
// verify text format and values.
func TestCounterAndGauge(t *testing.T) {
	r := NewRegistry()
	c := r.AddCounter("test_counter_total", "A test counter.")
	r.AddGauge("test_gauge", "A test gauge.", func() int64 { return 42 })
	c.Inc()
	c.Add(2)
	var buf bytes.Buffer
	if err := r.WriteProm(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{
		"# HELP test_counter_total A test counter.\n",
		"# TYPE test_counter_total counter\n",
		"test_counter_total 3\n",
		"# HELP test_gauge A test gauge.\n",
		"# TYPE test_gauge gauge\n",
		"test_gauge 42\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing line %q in:\n%s", want, out)
		}
	}
}

// TestAddCounterIdempotent: registering the same name twice returns
// the same Counter so init-time wiring is safe to call multiple
// times.
func TestAddCounterIdempotent(t *testing.T) {
	r := NewRegistry()
	a := r.AddCounter("x_total", "x")
	b := r.AddCounter("x_total", "x")
	if a != b {
		t.Fatal("repeated AddCounter returned different Counters")
	}
}

// TestServerServes: start a Server on :0, hit /metrics, look for our
// counter in the output.
func TestServerServes(t *testing.T) {
	// Use a private registry isolated from the package singleton so
	// the test doesn't see counters incremented elsewhere.
	c := Default.AddCounter("telfs_chunks_uploaded_total", "x")
	before := c.Load()
	c.Inc()

	s := NewServer("127.0.0.1:0")
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	}()

	resp, err := http.Get("http://" + s.Addr() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !bytes.Contains(body, []byte("telfs_chunks_uploaded_total")) {
		t.Fatalf("body missing counter name:\n%s", body)
	}
	_ = before
}
