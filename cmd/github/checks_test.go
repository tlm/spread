package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	. "gopkg.in/check.v1"
)

// checksSuite groups tests covering the Checks API client: request shape
// (method, URL, headers, body), response decoding, and error surfacing on
// transport and HTTP-level failures.
type checksSuite struct{}

var _ = Suite(&checksSuite{})

// decodeJSON parses a JSON object string into a generic map so tests can
// assert wire-level field names without coupling to the request struct.
func decodeJSON(c *C, body string) map[string]any {
	var m map[string]any
	err := json.NewDecoder(strings.NewReader(body)).Decode(&m)
	c.Assert(err, IsNil, Commentf("body: %q", body))
	return m
}

// errorResponse builds a non-2xx [http.Response] with an empty body for use
// by fakeDoer.
func errorResponse(status int) *http.Response {
	return &http.Response{
		Body:       io.NopCloser(strings.NewReader("")),
		Header:     make(http.Header),
		StatusCode: status,
	}
}

// newTestClient returns a checksClient wired to the given fakeDoer and a
// fixed set of credentials, repo, and SHA so tests can assert request URLs
// and Authorization headers verbatim.
func newTestClient(d *fakeDoer) *checksClient {
	api, _ := url.Parse("https://api.example.com")
	return &checksClient{
		apiURL: api,
		doer:   d,
		repo:   "owner/repo",
		sha:    "deadbeef",
		token:  "test-token",
	}
}

// okResponse builds a 200 [http.Response] with the given JSON body string
// for use by fakeDoer.
func okResponse(body string) *http.Response {
	return &http.Response{
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
		StatusCode: http.StatusOK,
	}
}

func Test(t *testing.T) {
	TestingT(t)
}

// TestCompleteError verifies that a non-2xx response from the GitHub API is
// surfaced as a wrapped error mentioning the check run id, so callers can
// attribute the failure without inspecting the wrapped chain.
func (s *checksSuite) TestCompleteError(c *C) {
	d := &fakeDoer{response: errorResponse(http.StatusForbidden)}
	cl := newTestClient(d)

	err := cl.complete(42, conclusionSuccess)
	c.Assert(err, NotNil)
	c.Check(err, ErrorMatches, `cannot patch check run 42:.*`)
}

// TestCompleteRequest verifies that complete() issues a PATCH to the correct
// Check Run URL with a body containing only the conclusion and status. The
// other checkRunRequest fields must be omitempty so the wire never carries
// stale or unintended values.
func (s *checksSuite) TestCompleteRequest(c *C) {
	d := &fakeDoer{}
	cl := newTestClient(d)

	err := cl.complete(42, conclusionSuccess)
	c.Assert(err, IsNil)

	c.Check(d.request.Method, Equals, http.MethodPatch)
	c.Check(
		d.request.URL.String(),
		Equals,
		"https://api.example.com/repos/owner/repo/check-runs/42",
	)
	c.Check(decodeJSON(c, d.body), DeepEquals, map[string]any{
		"conclusion": conclusionSuccess.String(),
		"status":     statusCompleted,
	})
}

// TestConclusionForAborted maps the aborted status to the cancelled
// conclusion, used for tasks the runner never picked up or that were
// skipped due to upstream prepare failures.
func (s *checksSuite) TestConclusionForAborted(c *C) {
	conc, ok := conclusionFor("aborted")
	c.Check(ok, Equals, true)
	c.Check(conc, Equals, conclusionCancelled)
}

// TestConclusionForEmpty returns ok=false for an empty status so the caller
// can distinguish "unmapped" from a real conclusion.
func (s *checksSuite) TestConclusionForEmpty(c *C) {
	_, ok := conclusionFor("")
	c.Check(ok, Equals, false)
}

// TestConclusionForFailed maps the failed status to the failure conclusion,
// used for tasks where prepare, execute, or restore returned an error.
func (s *checksSuite) TestConclusionForFailed(c *C) {
	conc, ok := conclusionFor("failed")
	c.Check(ok, Equals, true)
	c.Check(conc, Equals, conclusionFailure)
}

// TestConclusionForPassed maps the passed status to the success conclusion,
// used for tasks whose execute phase completed without error.
func (s *checksSuite) TestConclusionForPassed(c *C) {
	conc, ok := conclusionFor("passed")
	c.Check(ok, Equals, true)
	c.Check(conc, Equals, conclusionSuccess)
}

// TestConclusionForUnknown returns ok=false for any status the mapping does
// not recognise. Lets the caller surface a precise error rather than
// silently translating to a default conclusion.
func (s *checksSuite) TestConclusionForUnknown(c *C) {
	_, ok := conclusionFor("unknown")
	c.Check(ok, Equals, false)
}

// TestCreateCompletedRequest verifies that createCompleted() issues a POST
// with a body carrying head_sha, name, conclusion, and status=completed in a
// single request — used by the wrapper for tasks emitting task_finished
// without a prior task_started.
func (s *checksSuite) TestCreateCompletedRequest(c *C) {
	d := &fakeDoer{}
	cl := newTestClient(d)

	err := cl.createCompleted("task/1", conclusionFailure)
	c.Assert(err, IsNil)

	c.Check(d.request.Method, Equals, http.MethodPost)
	c.Check(
		d.request.URL.String(),
		Equals,
		"https://api.example.com/repos/owner/repo/check-runs",
	)
	c.Check(decodeJSON(c, d.body), DeepEquals, map[string]any{
		"conclusion": conclusionFailure.String(),
		"head_sha":   "deadbeef",
		"name":       "task/1",
		"status":     statusCompleted,
	})
}

// TestCreateError verifies non-2xx surfaces a wrapped error mentioning the
// check run name.
func (s *checksSuite) TestCreateError(c *C) {
	d := &fakeDoer{
		response: errorResponse(http.StatusUnprocessableEntity),
	}
	cl := newTestClient(d)

	_, err := cl.create("task/1")
	c.Assert(err, NotNil)
	c.Check(err, ErrorMatches, `cannot create check run "task/1":.*`)
}

// TestCreateRequest verifies that create() issues a POST with a body
// carrying head_sha, name, and status=in_progress.
func (s *checksSuite) TestCreateRequest(c *C) {
	d := &fakeDoer{response: okResponse(`{"id": 12345}`)}
	cl := newTestClient(d)

	_, err := cl.create("task/1")
	c.Assert(err, IsNil)

	c.Check(d.request.Method, Equals, http.MethodPost)
	c.Check(
		d.request.URL.String(),
		Equals,
		"https://api.example.com/repos/owner/repo/check-runs",
	)
	c.Check(decodeJSON(c, d.body), DeepEquals, map[string]any{
		"head_sha": "deadbeef",
		"name":     "task/1",
		"status":   statusInProgress,
	})
}

// TestCreateReturnsID confirms the id is decoded from the response body and
// returned to the caller. The wrapper uses this id as the Check Run handle
// for the later PATCH on task_finished, so a decode failure or mismatch
// would orphan check runs.
func (s *checksSuite) TestCreateReturnsID(c *C) {
	d := &fakeDoer{response: okResponse(`{"id": 12345}`)}
	cl := newTestClient(d)

	id, err := cl.create("task/1")
	c.Assert(err, IsNil)
	c.Check(id, Equals, int64(12345))
}

// TestDoSetsHeaders verifies every API request carries the four headers
// GitHub's REST API expects: Accept (vendor MIME variant), Authorization
// (Bearer token), Content-Type (JSON), and X-GitHub-Api-Version.
func (s *checksSuite) TestDoSetsHeaders(c *C) {
	d := &fakeDoer{}
	cl := newTestClient(d)

	body, err := cl.do(http.MethodPost, cl.apiURL, strings.NewReader(""))
	c.Assert(err, IsNil)
	body.Close()

	h := d.request.Header
	c.Check(h.Get(headerAccept), Equals, mediaTypeGitHubJSON)
	c.Check(h.Get(headerAuthorization), Equals, "Bearer test-token")
	c.Check(h.Get(headerContentType), Equals, mediaTypeJSON)
	c.Check(h.Get(headerGitHubAPIVersion), Equals, githubAPIVersion)
}

// TestDoTransportError verifies that a transport-level error from the doer
// is wrapped (with %w) so callers can unwrap and inspect the underlying
// cause via [errors.Is].
func (s *checksSuite) TestDoTransportError(c *C) {
	sentinel := errors.New("network down")
	d := &fakeDoer{err: sentinel}
	cl := newTestClient(d)

	_, err := cl.do(http.MethodGet, cl.apiURL, strings.NewReader(""))
	c.Assert(err, NotNil)
	c.Check(errors.Is(err, sentinel), Equals, true)
}
