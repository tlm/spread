package spread_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	. "gopkg.in/check.v1"

	"github.com/snapcore/spread/spread"
)

// cieventsSuite groups tests covering the CI event stream writer:
// per-event JSON shapes, JSONL framing, concurrency safety, and the
// nil-receiver no-op contract.
type cieventsSuite struct{}

// nopCloser adapts a [bytes.Buffer] to [io.WriteCloser] so it can stand in as
// the writer's sink in tests without involving the filesystem.
type nopCloser struct{ *bytes.Buffer }

var _ = Suite(&cieventsSuite{})

func (nopCloser) Close() error {
	return nil
}

// decodeLine parses a single JSON line into a generic map. Decoding into
// map[string]any (rather than the original struct) ensures the
// assertions exercise the wire-level field names from the `json:` tags, which
// are the consumer-facing contract.
func decodeLine(c *C, line string) map[string]any {
	var m map[string]any
	err := json.Unmarshal([]byte(line), &m)
	c.Assert(err, IsNil, Commentf("line: %q", line))
	return m
}

// decodeLines splits the buffer on newlines and decodes each line. Used by
// tests that emit more than one event and need to assert per-event shapes.
func decodeLines(c *C, buf *bytes.Buffer) []map[string]any {
	raw := strings.TrimRight(buf.String(), "\n")
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, "\n")
	out := make([]map[string]any, len(parts))
	for i, line := range parts {
		out[i] = decodeLine(c, line)
	}
	return out
}

// newTestCIEventsWriter returns a [spread.CIEventsWriter] backed by an
// in-memory buffer and a fixed clock, along with the buffer so tests can
// inspect what was written. The fixed clock returns 2026-05-17T08:18:34Z,
// which each test asserts verbatim in its expected ts field.
func newTestCIEventsWriter() (*spread.CIEventsWriter, *bytes.Buffer) {
	var buf bytes.Buffer
	clock := func() time.Time {
		return time.Date(2026, 5, 17, 8, 18, 34, 0, time.UTC)
	}
	return spread.NewCIEventsWriter(nopCloser{&buf}, clock), &buf
}

// TestConcurrentEmitDoesNotCorrupt locks down the writer's concurrency
// contract: many goroutines emitting in parallel must produce a stream where
// every line parses as valid JSON and the total event count matches.
// Spread's runner emits from multiple worker goroutines, so any interleaving
// at the byte level would silently corrupt the consumer's view of the run.
// The [bytes.Buffer] sink is itself not thread-safe, so a passing run here
// proves the writer's mutex is doing the work.
func (s *cieventsSuite) TestConcurrentEmitDoesNotCorrupt(c *C) {
	w, buf := newTestCIEventsWriter()

	const goroutines = 16
	const eventsPerGoroutine = 64

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < eventsPerGoroutine; j++ {
				w.EmitTaskStarted(fmt.Sprintf("g%d-e%d", id, j))
			}
		}(i)
	}
	wg.Wait()

	events := decodeLines(c, buf)
	c.Assert(events, HasLen, goroutines*eventsPerGoroutine)
	for _, m := range events {
		c.Check(m["event"], Equals, "task_started")
	}
}

// TestEmitRunFinished verifies the wire shape of the run_finished event: the
// three outcome counters (passed/failed/aborted) are emitted with the names
// consumers depend on for run-level rollup.
func (s *cieventsSuite) TestEmitRunFinished(c *C) {
	w, buf := newTestCIEventsWriter()
	w.EmitRunFinished(5, 2, 1)

	events := decodeLines(c, buf)
	c.Assert(events, HasLen, 1)
	c.Check(events[0], DeepEquals, map[string]any{
		"event":   "run_finished",
		"passed":  float64(5),
		"failed":  float64(2),
		"aborted": float64(1),
		"ts":      "2026-05-17T08:18:34Z",
	})
}

// TestEmitRunStarted verifies the wire shape of the run_started event:
// event name, schema_version pinned to 1, the seed and task_count arguments
// passed through verbatim, and an RFC3339 timestamp produced by the clock.
func (s *cieventsSuite) TestEmitRunStarted(c *C) {
	w, buf := newTestCIEventsWriter()
	w.EmitRunStarted(1234, 7)

	events := decodeLines(c, buf)
	c.Assert(events, HasLen, 1)
	c.Check(events[0], DeepEquals, map[string]any{
		"event":          "run_started",
		"schema_version": float64(1),
		"seed":           float64(1234),
		"task_count":     float64(7),
		"ts":             "2026-05-17T08:18:34Z",
	})
}

// TestEmitTaskFinishedAbortedOmitsOptionalFields pins the omitempty contract
// for the abort path. A task that was never picked up by a worker has no
// attempt number, no duration, and no failed phase — those fields must be
// absent from the JSON entirely, not present-but-zero. Consumers distinguish
// "field missing" from "field is 0" when deciding what to render. The
// DeepEquals against an exact four-field map enforces that absence: any
// stray field would fail the comparison.
func (s *cieventsSuite) TestEmitTaskFinishedAbortedOmitsOptionalFields(c *C) {
	w, buf := newTestCIEventsWriter()
	w.EmitTaskFinished("id", "aborted", "", 0, 0, 0)

	events := decodeLines(c, buf)
	c.Assert(events, HasLen, 1)
	c.Check(events[0], DeepEquals, map[string]any{
		"event":  "task_finished",
		"id":     "id",
		"status": "aborted",
		"ts":     "2026-05-17T08:18:34Z",
	})
}

// TestEmitTaskFinishedFailed covers the failure-path task_finished shape:
// when status is "failed" the failed_phase field must be populated so the
// consumer can attribute the failure to a specific phase.
func (s *cieventsSuite) TestEmitTaskFinishedFailed(c *C) {
	w, buf := newTestCIEventsWriter()
	w.EmitTaskFinished("id", "failed", "execute", 2, 3, 500)

	events := decodeLines(c, buf)
	c.Assert(events, HasLen, 1)
	c.Check(events[0], DeepEquals, map[string]any{
		"event":          "task_finished",
		"id":             "id",
		"status":         "failed",
		"failed_phase":   "execute",
		"attempt":        float64(2),
		"attempts_total": float64(3),
		"duration_ms":    float64(500),
		"ts":             "2026-05-17T08:18:34Z",
	})
}

// TestEmitTaskFinishedPassed covers the happy-path task_finished shape: the
// status is "passed", all populated optional fields (attempt, attempts_total,
// duration_ms) appear on the wire, and failed_phase is omitted because no
// phase failed. The DeepEquals against an exact map enforces the absence of
// failed_phase implicitly.
func (s *cieventsSuite) TestEmitTaskFinishedPassed(c *C) {
	w, buf := newTestCIEventsWriter()
	w.EmitTaskFinished("id", "passed", "", 1, 2, 12345)

	events := decodeLines(c, buf)
	c.Assert(events, HasLen, 1)
	c.Check(events[0], DeepEquals, map[string]any{
		"event":          "task_finished",
		"id":             "id",
		"status":         "passed",
		"attempt":        float64(1),
		"attempts_total": float64(2),
		"duration_ms":    float64(12345),
		"ts":             "2026-05-17T08:18:34Z",
	})
}

// TestEmitTaskPhase verifies the wire shape of the task_phase event. The
// phase value is passed through unmodified — the writer does not validate it
// against the allowed set (prepare/execute/restore); that discipline lives at
// the call sites in the runner.
func (s *cieventsSuite) TestEmitTaskPhase(c *C) {
	w, buf := newTestCIEventsWriter()
	w.EmitTaskPhase("lxd:ubuntu-24.04:tests/foo:jammy", "execute")

	events := decodeLines(c, buf)
	c.Assert(events, HasLen, 1)
	c.Check(events[0], DeepEquals, map[string]any{
		"event": "task_phase",
		"id":    "lxd:ubuntu-24.04:tests/foo:jammy",
		"phase": "execute",
		"ts":    "2026-05-17T08:18:34Z",
	})
}

// TestEmitTaskStarted verifies the wire shape of the task_started event and
// confirms the task ID is preserved exactly as provided — consumers key their
// per-task state (e.g. one GitHub Check Run per task) on this field, so any
// mangling here would break correlation across the event stream.
func (s *cieventsSuite) TestEmitTaskStarted(c *C) {
	w, buf := newTestCIEventsWriter()
	w.EmitTaskStarted("lxd:ubuntu-24.04:tests/foo:jammy")

	events := decodeLines(c, buf)
	c.Assert(events, HasLen, 1)
	c.Check(events[0], DeepEquals, map[string]any{
		"event": "task_started",
		"id":    "lxd:ubuntu-24.04:tests/foo:jammy",
		"ts":    "2026-05-17T08:18:34Z",
	})
}

// TestNewlineDelimited verifies the JSONL framing contract: each event ends
// with exactly one newline, events are written in emission order, and the
// final event also has its terminating newline (so consumers tail-ing the
// file with line-buffered readers never see a truncated last event).
func (s *cieventsSuite) TestNewlineDelimited(c *C) {
	w, buf := newTestCIEventsWriter()
	w.EmitRunStarted(1, 1)
	w.EmitTaskStarted("id")
	w.EmitRunFinished(1, 0, 0)

	raw := buf.String()
	c.Check(strings.HasSuffix(raw, "\n"), Equals, true)
	c.Check(strings.Count(raw, "\n"), Equals, 3)

	events := decodeLines(c, buf)
	c.Assert(events, HasLen, 3)
	c.Check(events[0]["event"], Equals, "run_started")
	c.Check(events[1]["event"], Equals, "task_started")
	c.Check(events[2]["event"], Equals, "run_finished")
}

// TestNilWriterIsNoop verifies that every method on [spread.CIEventsWriter]
// tolerates a nil receiver. The runner stores a nil writer when -ci-events is
// not passed, and the emission sites in the runner call methods directly
// rather than guarding each call site with a nil check — so the nil-tolerance
// has to live on the writer itself.
func (s *cieventsSuite) TestNilWriterIsNoop(c *C) {
	var w *spread.CIEventsWriter
	w.EmitRunStarted(0, 0)
	w.EmitRunFinished(0, 0, 0)
	w.EmitTaskStarted("id")
	w.EmitTaskPhase("id", "prepare")
	w.EmitTaskFinished("id", "passed", "", 0, 0, 0)
	c.Check(w.Close(), IsNil)
}
