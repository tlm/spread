package main

import (
	"errors"
	"io"
	"net/http"
	"os"

	. "gopkg.in/check.v1"
)

// mainSuite groups tests covering the wrapper's standalone functions:
// splitArgs, lookupEnv, drainBuffer, dispatch, and sweep.
type mainSuite struct{}

var _ = Suite(&mainSuite{})

// TestDispatchInvalidJSON returns an error wrapping the offending line when
// the input is not valid JSON. The wrapped message must include the raw
// line so the operator can see what failed.
func (s *mainSuite) TestDispatchInvalidJSON(c *C) {
	d := &fakeDoer{}
	cl := newTestClient(d)

	err := dispatch([]byte("not json"), cl, map[string]int64{})
	c.Assert(err, NotNil)
	c.Check(err, ErrorMatches, `cannot decode event "not json":.*`)
	c.Check(d.request, IsNil)
}

// TestDispatchTaskFinishedAPIError surfaces the API error verbatim when
// PATCH fails. The runs map is unchanged so a retry on the next invocation
// (or the end-of-run sweep) still sees the orphan.
func (s *mainSuite) TestDispatchTaskFinishedAPIError(c *C) {
	d := &fakeDoer{response: errorResponse(http.StatusInternalServerError)}
	cl := newTestClient(d)
	runs := map[string]int64{"task/1": 123}

	line := []byte(`{"event":"task_finished","id":"task/1","status":"passed"}`)
	err := dispatch(line, cl, runs)
	c.Assert(err, NotNil)
	c.Check(d.request.Method, Equals, http.MethodPatch)
	c.Check(runs, DeepEquals, map[string]int64{"task/1": 123})
}

// TestDispatchTaskFinishedAfterStart issues a PATCH against the previously
// recorded Check Run id and removes the entry from the runs map.
func (s *mainSuite) TestDispatchTaskFinishedAfterStart(c *C) {
	d := &fakeDoer{}
	cl := newTestClient(d)
	runs := map[string]int64{"task/1": 123}

	line := []byte(`{"event":"task_finished","id":"task/1","status":"passed"}`)
	err := dispatch(line, cl, runs)
	c.Assert(err, IsNil)
	c.Check(d.request.Method, Equals, http.MethodPatch)
	c.Check(
		d.request.URL.String(),
		Equals,
		"https://api.example.com/repos/owner/repo/check-runs/123",
	)
	c.Check(runs, DeepEquals, map[string]int64{})
}

// TestDispatchTaskFinishedMissingID rejects the event up front when the id
// is absent, before any API call is attempted.
func (s *mainSuite) TestDispatchTaskFinishedMissingID(c *C) {
	d := &fakeDoer{}
	cl := newTestClient(d)

	line := []byte(`{"event":"task_finished","status":"passed"}`)
	err := dispatch(line, cl, map[string]int64{})
	c.Assert(err, NotNil)
	c.Check(err, ErrorMatches, `task_finished event missing id:.*`)
	c.Check(d.request, IsNil)
}

// TestDispatchTaskFinishedUnknownStatus rejects the event when the status
// is not one of the three known spread outcomes; the error names the id
// and the offending status so the operator can fix the producer.
func (s *mainSuite) TestDispatchTaskFinishedUnknownStatus(c *C) {
	d := &fakeDoer{}
	cl := newTestClient(d)

	line := []byte(`{"event":"task_finished","id":"task/1","status":"weird"}`)
	err := dispatch(line, cl, map[string]int64{})
	c.Assert(err, NotNil)
	c.Check(
		err,
		ErrorMatches,
		`task_finished for "task/1" has unknown status "weird"`,
	)
	c.Check(d.request, IsNil)
}

// TestDispatchTaskFinishedWithoutStart issues a single POST against the
// create-completed endpoint when no prior task_started recorded the id —
// the optimisation that avoids a wasted POST+PATCH pair for tasks the
// runner never picked up.
func (s *mainSuite) TestDispatchTaskFinishedWithoutStart(c *C) {
	d := &fakeDoer{}
	cl := newTestClient(d)
	runs := map[string]int64{}

	line := []byte(`{"event":"task_finished","id":"task/1","status":"aborted"}`)
	err := dispatch(line, cl, runs)
	c.Assert(err, IsNil)
	c.Check(d.request.Method, Equals, http.MethodPost)
	c.Check(
		d.request.URL.String(),
		Equals,
		"https://api.example.com/repos/owner/repo/check-runs",
	)
	c.Check(runs, DeepEquals, map[string]int64{})
}

// TestDispatchTaskStartedAPIError surfaces the API error and leaves the
// runs map untouched so a later task_finished for the same id falls into
// the without-start path rather than PATCHing a non-existent Check Run.
func (s *mainSuite) TestDispatchTaskStartedAPIError(c *C) {
	d := &fakeDoer{response: errorResponse(http.StatusInternalServerError)}
	cl := newTestClient(d)
	runs := map[string]int64{}

	line := []byte(`{"event":"task_started","id":"task/1"}`)
	err := dispatch(line, cl, runs)
	c.Assert(err, NotNil)
	c.Check(runs, DeepEquals, map[string]int64{})
}

// TestDispatchTaskStartedMissingID rejects the event up front when the id
// is absent, before any API call is attempted.
func (s *mainSuite) TestDispatchTaskStartedMissingID(c *C) {
	d := &fakeDoer{}
	cl := newTestClient(d)

	err := dispatch([]byte(`{"event":"task_started"}`), cl, map[string]int64{})
	c.Assert(err, NotNil)
	c.Check(err, ErrorMatches, `task_started event missing id:.*`)
	c.Check(d.request, IsNil)
}

// TestDispatchTaskStartedRecordsID issues a POST to create the Check Run
// and stores the returned numeric id under the task id key so a subsequent
// task_finished can PATCH it.
func (s *mainSuite) TestDispatchTaskStartedRecordsID(c *C) {
	d := &fakeDoer{response: okResponse(`{"id": 12345}`)}
	cl := newTestClient(d)
	runs := map[string]int64{}

	line := []byte(`{"event":"task_started","id":"task/1"}`)
	err := dispatch(line, cl, runs)
	c.Assert(err, IsNil)
	c.Check(d.request.Method, Equals, http.MethodPost)
	c.Check(runs, DeepEquals, map[string]int64{"task/1": 12345})
}

// TestDispatchUnknownEvent silently ignores events the wrapper does not
// handle, so the contract stays forward-compatible with future event types
// emitted by spread.
func (s *mainSuite) TestDispatchUnknownEvent(c *C) {
	d := &fakeDoer{}
	cl := newTestClient(d)

	err := dispatch([]byte(`{"event":"task_phase"}`), cl, map[string]int64{})
	c.Assert(err, IsNil)
	c.Check(d.request, IsNil)
}

// TestDrainBufferAllPartial leaves a buffer with no terminator untouched
// and invokes the handler zero times — the line is incomplete and must
// wait for more bytes.
func (s *mainSuite) TestDrainBufferAllPartial(c *C) {
	buf := []byte("foo")
	var got []string
	err := drainBuffer(&buf, func(line []byte) error {
		got = append(got, string(line))
		return nil
	})
	c.Assert(err, IsNil)
	c.Check(got, IsNil)
	c.Check(string(buf), Equals, "foo")
}

// TestDrainBufferEmpty does nothing when given an empty buffer and never
// calls the handler.
func (s *mainSuite) TestDrainBufferEmpty(c *C) {
	buf := []byte{}
	called := false
	err := drainBuffer(&buf, func(line []byte) error {
		called = true
		return nil
	})
	c.Assert(err, IsNil)
	c.Check(called, Equals, false)
	c.Check(buf, HasLen, 0)
}

// TestDrainBufferHandleErrorStops returns immediately when the handler
// errors. The line that produced the error has already been consumed from
// the buffer; lines after it are not handled and remain in the buffer
// untouched.
func (s *mainSuite) TestDrainBufferHandleErrorStops(c *C) {
	buf := []byte("a\nb\nc\n")
	sentinel := errors.New("stop here")
	var got []string
	err := drainBuffer(&buf, func(line []byte) error {
		got = append(got, string(line))
		if string(line) == "b" {
			return sentinel
		}
		return nil
	})
	c.Assert(errors.Is(err, sentinel), Equals, true)
	c.Check(got, DeepEquals, []string{"a", "b"})
	c.Check(string(buf), Equals, "c\n")
}

// TestDrainBufferOneCompleteLine hands the line to the handler with the
// trailing newline stripped and clears the buffer.
func (s *mainSuite) TestDrainBufferOneCompleteLine(c *C) {
	buf := []byte("foo\n")
	var got []string
	err := drainBuffer(&buf, func(line []byte) error {
		got = append(got, string(line))
		return nil
	})
	c.Assert(err, IsNil)
	c.Check(got, DeepEquals, []string{"foo"})
	c.Check(buf, HasLen, 0)
}

// TestDrainBufferSkipsEmptyLines does not invoke the handler for zero-
// length lines (consecutive newlines). The skip is significant for the
// caller's contract: every handler call sees a non-empty payload.
func (s *mainSuite) TestDrainBufferSkipsEmptyLines(c *C) {
	buf := []byte("\n\nfoo\n")
	var got []string
	err := drainBuffer(&buf, func(line []byte) error {
		got = append(got, string(line))
		return nil
	})
	c.Assert(err, IsNil)
	c.Check(got, DeepEquals, []string{"foo"})
	c.Check(buf, HasLen, 0)
}

// TestDrainBufferTrailingPartial hands complete lines to the handler and
// leaves any trailing incomplete bytes in the buffer for the next call.
func (s *mainSuite) TestDrainBufferTrailingPartial(c *C) {
	buf := []byte("a\nbb")
	var got []string
	err := drainBuffer(&buf, func(line []byte) error {
		got = append(got, string(line))
		return nil
	})
	c.Assert(err, IsNil)
	c.Check(got, DeepEquals, []string{"a"})
	c.Check(string(buf), Equals, "bb")
}

// TestDrainBufferTwoCompleteLines hands both lines to the handler in
// emission order and clears the buffer.
func (s *mainSuite) TestDrainBufferTwoCompleteLines(c *C) {
	buf := []byte("a\nb\n")
	var got []string
	err := drainBuffer(&buf, func(line []byte) error {
		got = append(got, string(line))
		return nil
	})
	c.Assert(err, IsNil)
	c.Check(got, DeepEquals, []string{"a", "b"})
	c.Check(buf, HasLen, 0)
}

// TestLookupEnvEmpty treats an env var set to the empty string as missing.
// The wrapper's contract is that all required values must be non-empty;
// distinguishing "unset" from "set to empty" would only invite confusion
// at the call sites.
func (s *mainSuite) TestLookupEnvEmpty(c *C) {
	const name = "SPREAD_GH_TEST_VAR_EMPTY"
	os.Setenv(name, "")
	defer os.Unsetenv(name)
	v, ok := lookupEnv(name)
	c.Check(ok, Equals, false)
	c.Check(v, Equals, "")
}

// TestLookupEnvSet returns the value and ok=true when the variable is set
// to a non-empty string.
func (s *mainSuite) TestLookupEnvSet(c *C) {
	const name = "SPREAD_GH_TEST_VAR_SET"
	os.Setenv(name, "hello")
	defer os.Unsetenv(name)
	v, ok := lookupEnv(name)
	c.Check(ok, Equals, true)
	c.Check(v, Equals, "hello")
}

// TestLookupEnvUnset returns ok=false when the variable is not present in
// the environment.
func (s *mainSuite) TestLookupEnvUnset(c *C) {
	const name = "SPREAD_GH_TEST_VAR_THAT_NEVER_EXISTS"
	os.Unsetenv(name)
	_, ok := lookupEnv(name)
	c.Check(ok, Equals, false)
}

// TestSplitArgsBasic returns the args before the separator as wrapper-
// specific and the args after as spread args.
func (s *mainSuite) TestSplitArgsBasic(c *C) {
	wrapper, spread, err := splitArgs([]string{"a", "--", "b", "c"})
	c.Assert(err, IsNil)
	c.Check(wrapper, DeepEquals, []string{"a"})
	c.Check(spread, DeepEquals, []string{"b", "c"})
}

// TestSplitArgsEmpty rejects an empty argument list since the separator is
// mandatory.
func (s *mainSuite) TestSplitArgsEmpty(c *C) {
	_, _, err := splitArgs([]string{})
	c.Assert(err, NotNil)
	c.Check(err, ErrorMatches, `missing -- separator.*`)
}

// TestSplitArgsMissingSeparator rejects input that lacks the literal `--`
// separator with a clear usage hint.
func (s *mainSuite) TestSplitArgsMissingSeparator(c *C) {
	_, _, err := splitArgs([]string{"a", "b"})
	c.Assert(err, NotNil)
	c.Check(err, ErrorMatches, `missing -- separator.*`)
}

// TestSplitArgsNoArgsAfter returns an empty spread-args slice when the
// separator is present but nothing follows it.
func (s *mainSuite) TestSplitArgsNoArgsAfter(c *C) {
	wrapper, spread, err := splitArgs([]string{"a", "--"})
	c.Assert(err, IsNil)
	c.Check(wrapper, DeepEquals, []string{"a"})
	c.Check(spread, HasLen, 0)
}

// TestSplitArgsNoArgsBefore returns an empty wrapper-args slice when the
// separator is the first argument.
func (s *mainSuite) TestSplitArgsNoArgsBefore(c *C) {
	wrapper, spread, err := splitArgs([]string{"--", "b"})
	c.Assert(err, IsNil)
	c.Check(wrapper, HasLen, 0)
	c.Check(spread, DeepEquals, []string{"b"})
}

// TestSplitArgsSeparatorOnly handles a lone separator: both partitions are
// empty slices, no error.
func (s *mainSuite) TestSplitArgsSeparatorOnly(c *C) {
	wrapper, spread, err := splitArgs([]string{"--"})
	c.Assert(err, IsNil)
	c.Check(wrapper, HasLen, 0)
	c.Check(spread, HasLen, 0)
}

// TestSweepContinuesOnError keeps issuing PATCH requests for the remaining
// entries even after one of them fails, so a single bad Check Run id at
// shutdown cannot orphan the others.
func (s *mainSuite) TestSweepContinuesOnError(c *C) {
	d := &recordingDoer{
		responses: []recordingDoerResponse{
			{resp: errorResponse(http.StatusInternalServerError)},
			{resp: okResponseRC()},
		},
	}
	cl := newTestClient(nil)
	cl.doer = d
	runs := map[string]int64{"task/1": 1, "task/2": 2}

	sweep(cl, runs)
	c.Assert(d.requests, HasLen, 2)
	for _, req := range d.requests {
		c.Check(req.Method, Equals, http.MethodPatch)
	}
}

// TestSweepEmpty issues no API requests when the runs map is empty.
func (s *mainSuite) TestSweepEmpty(c *C) {
	d := &recordingDoer{}
	cl := newTestClient(nil)
	cl.doer = d
	runs := map[string]int64{}

	sweep(cl, runs)
	c.Check(d.requests, HasLen, 0)
}

// TestSweepPatchesAllAsCancelled issues a PATCH for every entry in the
// runs map, each carrying conclusion=cancelled in its JSON body so the
// orphan Check Runs land in a terminal state.
func (s *mainSuite) TestSweepPatchesAllAsCancelled(c *C) {
	d := &recordingDoer{
		responses: []recordingDoerResponse{
			{resp: okResponseRC()},
			{resp: okResponseRC()},
		},
	}
	cl := newTestClient(nil)
	cl.doer = d
	runs := map[string]int64{"task/1": 1, "task/2": 2}

	sweep(cl, runs)
	c.Assert(d.requests, HasLen, 2)
	for _, req := range d.requests {
		c.Check(req.Method, Equals, http.MethodPatch)
		body, _ := io.ReadAll(req.Body)
		req.Body.Close()
		c.Check(
			decodeJSON(c, string(body)),
			DeepEquals,
			map[string]any{
				"conclusion": conclusionCancelled.String(),
				"status":     statusCompleted,
			},
		)
	}
}

// okResponseRC builds a fresh 200 [http.Response] suitable for use as one
// entry in [recordingDoer]'s response queue. Separate from okResponse
// because every queued response needs its own non-shared Body.
func okResponseRC() *http.Response {
	return okResponse("")
}
