package main

import (
	"fmt"
	"io"
	"net/http"
)

// fakeDoer is a [doer] implementation that captures the request passed to
// Do and returns a configured response or error. Single-request use — tests
// that need multiple calls should use [recordingDoer] instead.
type fakeDoer struct {
	body     string
	err      error
	request  *http.Request
	response *http.Response
}

// recordingDoer is a [doer] that returns a configured sequence of responses
// in order and records every request it received. Used by tests that need
// to exercise multiple API calls in one operation (e.g. sweep).
type recordingDoer struct {
	requests  []*http.Request
	responses []recordingDoerResponse
}

// recordingDoerResponse is one element of [recordingDoer]'s response queue.
// Exactly one of resp or err is expected to be set per element.
type recordingDoerResponse struct {
	err  error
	resp *http.Response
}

// Do records the request (consuming its body into f.body) and returns the
// configured response or error.
func (f *fakeDoer) Do(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		data, _ := io.ReadAll(req.Body)
		f.body = string(data)
		req.Body.Close()
	}
	f.request = req
	if f.err != nil {
		return nil, f.err
	}
	if f.response == nil {
		return okResponse(""), nil
	}
	return f.response, nil
}

// Do records the request and returns the next configured response. If the
// queue is exhausted, returns an error so unexpected extra requests fail
// loudly rather than picking up a phantom default.
func (r *recordingDoer) Do(req *http.Request) (*http.Response, error) {
	r.requests = append(r.requests, req)
	idx := len(r.requests) - 1
	if idx >= len(r.responses) {
		return nil, fmt.Errorf(
			"recordingDoer: no response configured for request %d", idx,
		)
	}
	out := r.responses[idx]
	if out.err != nil {
		return nil, out.err
	}
	return out.resp, nil
}
