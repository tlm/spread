// Package main implements spread-github, a wrapper around the spread CLI
// that translates its CI event stream into GitHub Check Runs. This file
// holds the minimal HTTP client for the Checks REST API; the wrapper itself
// lives in main.go.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// checkRunRequest is the JSON body sent to POST and PATCH endpoints. Fields
// are omitempty so a single shape can serve both: POST sets name/head_sha/
// status; PATCH sets status/conclusion.
type checkRunRequest struct {
	Conclusion string `json:"conclusion,omitempty"`
	HeadSHA    string `json:"head_sha,omitempty"`
	Name       string `json:"name,omitempty"`
	Status     string `json:"status,omitempty"`
}

// checkRunResponse captures the fields we need from a created Check Run.
type checkRunResponse struct {
	ID int64 `json:"id"`
}

// checksClient posts and patches GitHub Check Runs for the configured
// repository and commit SHA.
type checksClient struct {
	apiURL *url.URL
	doer   doer
	repo   string
	sha    string
	token  string
}

// doer abstracts the single method of [http.Client] that checksClient relies
// on, so tests can substitute a fake transport without spinning up a real
// HTTP server.
type doer interface {
	Do(req *http.Request) (*http.Response, error)
}

const (
	apiURLDefault = "https://api.github.com"

	conclusionCancelled = "cancelled"
	conclusionFailure   = "failure"
	conclusionSuccess   = "success"

	githubAPIVersion = "2022-11-28"

	headerAccept           = "Accept"
	headerAuthorization    = "Authorization"
	headerContentType      = "Content-Type"
	headerGitHubAPIVersion = "X-GitHub-Api-Version"

	httpClientTimeout = 30 * time.Second

	mediaTypeGitHubJSON = "application/vnd.github+json"
	mediaTypeJSON       = "application/json"

	statusCompleted  = "completed"
	statusInProgress = "in_progress"
)

// complete patches an existing Check Run to status completed with the given
// conclusion.
func (c *checksClient) complete(id int64, conclusion string) error {
	body := checkRunRequest{
		Conclusion: conclusion,
		Status:     statusCompleted,
	}
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(body)
	if err != nil {
		return fmt.Errorf(
			"cannot encode patch body for check run %d: %w", id, err,
		)
	}

	idStr := strconv.FormatInt(id, 10)
	u := c.apiURL.JoinPath("repos", c.repo, "check-runs", idStr)
	respBody, err := c.do(http.MethodPatch, u, &buf)
	if err != nil {
		return fmt.Errorf("cannot patch check run %d: %w", id, err)
	}
	respBody.Close()
	return nil
}

// conclusionFor maps a spread task_finished status to the GitHub Check Run
// conclusion string. Returns the empty string for unknown statuses so the
// caller can surface a precise error.
func conclusionFor(status string) string {
	switch status {
	case "passed":
		return conclusionSuccess
	case "failed":
		return conclusionFailure
	case "aborted":
		return conclusionCancelled
	}
	return ""
}

// create posts a new Check Run with status in_progress and returns its
// numeric id, which the caller uses for the later PATCH on task_finished.
func (c *checksClient) create(name string) (int64, error) {
	body := checkRunRequest{
		HeadSHA: c.sha,
		Name:    name,
		Status:  statusInProgress,
	}
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(body)
	if err != nil {
		return 0, fmt.Errorf(
			"cannot encode create body for check run %q: %w", name, err,
		)
	}

	u := c.apiURL.JoinPath("repos", c.repo, "check-runs")
	respBody, err := c.do(http.MethodPost, u, &buf)
	if err != nil {
		return 0, fmt.Errorf("cannot create check run %q: %w", name, err)
	}
	defer respBody.Close()
	var resp checkRunResponse
	err = json.NewDecoder(respBody).Decode(&resp)
	if err != nil {
		return 0, fmt.Errorf("cannot decode check run response: %w", err)
	}
	return resp.ID, nil
}

// createCompleted posts a new Check Run already in the completed state. Used
// for tasks that emit task_finished with no prior task_started (e.g. jobs the
// runner never picked up), so the wrapper avoids a wasted POST+PATCH pair.
func (c *checksClient) createCompleted(name, conclusion string) error {
	body := checkRunRequest{
		Conclusion: conclusion,
		HeadSHA:    c.sha,
		Name:       name,
		Status:     statusCompleted,
	}
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(body)
	if err != nil {
		return fmt.Errorf(
			"cannot encode create body for completed check run %q: %w",
			name, err,
		)
	}

	u := c.apiURL.JoinPath("repos", c.repo, "check-runs")
	respBody, err := c.do(http.MethodPost, u, &buf)
	if err != nil {
		return fmt.Errorf("cannot create completed check run %q: %w", name, err)
	}
	respBody.Close()
	return nil
}

// defaultDoer returns a [http.Client] configured with the wrapper's default
// request timeout. Returned as the doer interface so production code reads
// uniformly with the test paths.
func defaultDoer() doer {
	return &http.Client{Timeout: httpClientTimeout}
}

// do issues an authenticated JSON request and returns the response body as
// an [io.ReadCloser] on a 2xx status. The caller is responsible for closing
// the returned body. On any non-2xx status or transport error the response
// body (if any) is closed and a nil ReadCloser is returned alongside the
// error. The caller is responsible for encoding the request body (typically
// as JSON) and passing it as an [io.Reader].
func (c *checksClient) do(
	method string,
	u *url.URL,
	body io.Reader,
) (io.ReadCloser, error) {
	req, err := http.NewRequest(method, u.String(), body)
	if err != nil {
		return nil, fmt.Errorf("cannot build request: %w", err)
	}
	req.Header.Set(headerAccept, mediaTypeGitHubJSON)
	req.Header.Set(headerAuthorization, "Bearer "+c.token)
	req.Header.Set(headerContentType, mediaTypeJSON)
	req.Header.Set(headerGitHubAPIVersion, githubAPIVersion)
	resp, err := c.doer.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s %s failed: %w", method, u, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, fmt.Errorf(
			"%s %s returned %d", method, u, resp.StatusCode,
		)
	}
	return resp.Body, nil
}
