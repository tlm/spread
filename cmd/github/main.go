// spread-github wraps a spread invocation and translates spread's CI event
// stream into GitHub Check Runs, one per spread task. The wrapper points
// spread at a temporary JSONL file via the SPREAD_CI_EVENTS_FILE environment
// variable, tails the file, and dispatches each event to the GitHub Checks
// REST API. spread's own stdout and stderr pass through unchanged so the
// human-readable run is preserved.
//
// Invocation: spread-github [wrapper-flags] -- <spread args>
//
// Required environment:
//   GITHUB_TOKEN, GITHUB_REPOSITORY, GITHUB_SHA
//
// The workflow that runs this binary must grant `checks: write`; without it
// every Check Run POST returns 403 and the wrapper exits non-zero (fail-fast).

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

const (
	envGitHubActions      = "GITHUB_ACTIONS"
	envGitHubAPIURL       = "GITHUB_API_URL"
	envGitHubRepository   = "GITHUB_REPOSITORY"
	envGitHubSHA          = "GITHUB_SHA"
	envGitHubToken        = "GITHUB_TOKEN"
	envSpreadBinary       = "SPREAD_BINARY"
	envSpreadCIEventsFile = "SPREAD_CI_EVENTS_FILE"
)

const tailPollInterval = 50 * time.Millisecond

func main() {
	code, err := run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
	}
	os.Exit(code)
}

// dispatch translates one decoded event into Check Runs API calls. Only
// task_started and task_finished produce API calls in v1; other events are
// ignored. Returns an error on the first API failure so the caller can fail
// fast.
func dispatch(line []byte, client *checksClient, runs map[string]int64) error {
	var ev map[string]any
	err := json.Unmarshal(line, &ev)
	if err != nil {
		return fmt.Errorf("cannot decode event %q: %w", string(line), err)
	}

	name, _ := ev["event"].(string)
	switch name {
	case "task_started":
		id, _ := ev["id"].(string)
		if id == "" {
			return fmt.Errorf("task_started event missing id: %s", line)
		}
		runID, err := client.create(id)
		if err != nil {
			return err
		}
		runs[id] = runID
	case "task_finished":
		id, _ := ev["id"].(string)
		if id == "" {
			return fmt.Errorf("task_finished event missing id: %s", line)
		}
		status, _ := ev["status"].(string)
		conc, ok := conclusionFor(status)
		if !ok {
			return fmt.Errorf(
				"task_finished for %q has unknown status %q",
				id, status,
			)
		}
		runID, found := runs[id]
		if !found {
			err := client.createCompleted(id, conc)
			if err != nil {
				return err
			}
			return nil
		}
		err := client.complete(runID, conc)
		if err != nil {
			return err
		}
		delete(runs, id)
	}
	return nil
}

// drainBuffer consumes complete newline-terminated lines from buf, invoking
// handle on each. Any trailing incomplete line is left in buf for the next
// drain.
func drainBuffer(buf *[]byte, handle func([]byte) error) error {
	for {
		idx := bytes.IndexByte(*buf, '\n')
		if idx < 0 {
			return nil
		}
		line := (*buf)[:idx]
		*buf = (*buf)[idx+1:]
		if len(line) == 0 {
			continue
		}
		err := handle(line)
		if err != nil {
			return err
		}
	}
}

// lookupEnv returns the named environment variable's value and a bool
// reporting whether it was set to a non-empty string. Empty values are
// treated as missing. Mirrors [os.LookupEnv]'s comma-ok shape but folds the
// "set but empty" case into the missing branch.
func lookupEnv(name string) (string, bool) {
	v := os.Getenv(name)
	return v, v != ""
}

// run is the main work function. Returns the desired process exit code and
// an error to print to stderr (if any). Exit code 0 with nil error means
// clean success; other combinations are surfaced by main.
func run() (int, error) {
	_, spreadArgs, err := splitArgs(os.Args[1:])
	if err != nil {
		return 1, err
	}
	actions, ok := lookupEnv(envGitHubActions)
	if !ok || actions != "true" {
		return 1, fmt.Errorf(
			"%s is not set to \"true\"; spread-github must run inside a "+
				"GitHub Actions workflow runner",
			envGitHubActions,
		)
	}
	token, ok := lookupEnv(envGitHubToken)
	if !ok {
		return 1, fmt.Errorf(
			"%s is required but not set; it is not auto-injected by GitHub "+
				"Actions — add `env:\n  %s: ${{ secrets.GITHUB_TOKEN }}` to "+
				"the workflow step that runs spread-github, and ensure the "+
				"workflow declares `permissions: checks: write`",
			envGitHubToken, envGitHubToken,
		)
	}
	repo, ok := lookupEnv(envGitHubRepository)
	if !ok {
		return 1, fmt.Errorf(
			"%s is required but not set; this is normally auto-injected by "+
				"the GitHub Actions runner, so its absence suggests the "+
				"runner environment is unusual or has been overridden",
			envGitHubRepository,
		)
	}
	sha, ok := lookupEnv(envGitHubSHA)
	if !ok {
		return 1, fmt.Errorf(
			"%s is required but not set; this is normally auto-injected by "+
				"the GitHub Actions runner, so its absence suggests the "+
				"runner environment is unusual or has been overridden",
			envGitHubSHA,
		)
	}
	apiURLStr := os.Getenv(envGitHubAPIURL)
	if apiURLStr == "" {
		apiURLStr = apiURLDefault
	}
	apiURL, err := url.Parse(apiURLStr)
	if err != nil {
		return 1, fmt.Errorf("invalid %s: %w", envGitHubAPIURL, err)
	}

	tmp, err := os.CreateTemp("", "spread-github-events-*.jsonl")
	if err != nil {
		return 1, fmt.Errorf("cannot create events file: %w", err)
	}
	eventsPath := tmp.Name()
	tmp.Close()
	defer os.Remove(eventsPath)

	eventsR, err := os.Open(eventsPath)
	if err != nil {
		return 1, fmt.Errorf("cannot open events file for reading: %w", err)
	}
	defer eventsR.Close()

	client := &checksClient{
		apiURL: apiURL,
		doer:   defaultDoer(),
		repo:   repo,
		sha:    sha,
		token:  token,
	}

	spreadBin := os.Getenv(envSpreadBinary)
	if spreadBin == "" {
		spreadBin = "spread"
	}
	cmd := exec.Command(spreadBin, spreadArgs...)
	cmd.Env = append(
		os.Environ(),
		fmt.Sprintf("%s=%s", envSpreadCIEventsFile, eventsPath),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Start()
	if err != nil {
		return 1, fmt.Errorf("cannot start %s: %w", spreadBin, err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		sig, ok := <-sigCh
		if !ok {
			return
		}
		cmd.Process.Signal(sig)
	}()

	spreadDone := make(chan struct{})
	var spreadErr error
	go func() {
		spreadErr = cmd.Wait()
		close(spreadDone)
	}()

	runs := map[string]int64{}
	tailErr := tailEvents(eventsR, spreadDone, func(line []byte) error {
		return dispatch(line, client, runs)
	})

	if tailErr != nil {
		cmd.Process.Signal(syscall.SIGINT)
	}
	<-spreadDone

	if tailErr == nil {
		sweep(client, runs)
	}

	if tailErr != nil {
		return 1, tailErr
	}

	var exitErr *exec.ExitError
	if errors.As(spreadErr, &exitErr) {
		return exitErr.ExitCode(), nil
	}

	if spreadErr != nil {
		return 1, fmt.Errorf("waiting for spread failed: %w", spreadErr)
	}

	return 0, nil
}

// splitArgs partitions argv around the literal `--` separator. Arguments
// before the separator are wrapper-specific; arguments after are forwarded
// verbatim to spread. The wrapper currently has no flags of its own but the
// separator is mandatory so the contract can grow without ambiguity.
func splitArgs(args []string) ([]string, []string, error) {
	for i, a := range args {
		if a == "--" {
			return args[:i], args[i+1:], nil
		}
	}
	return nil, nil, fmt.Errorf(
		"missing -- separator; usage: spread-github [flags] -- <spread args>",
	)
}

// sweep best-effort patches every Check Run still in runs to conclusion
// cancelled. Used on clean exit when spread emitted task_started but no
// corresponding task_finished (defensive against runner crashes). Sweep
// errors are logged to stderr and ignored; we are already exiting.
func sweep(client *checksClient, runs map[string]int64) {
	for id, runID := range runs {
		err := client.complete(runID, conclusionCancelled)
		if err != nil {
			fmt.Fprintf(
				os.Stderr,
				"warning: sweep of check run for %q failed: %v\n",
				id, err,
			)
		}
	}
}

// tailEvents reads JSONL lines from r as spread writes them and hands each
// complete line to handle. The loop exits when spreadDone has been closed
// AND the file has no further bytes to drain.
func tailEvents(
	r *os.File,
	spreadDone <-chan struct{},
	handle func([]byte) error,
) error {
	var buf []byte
	chunk := make([]byte, 4096)
	for {
		n, err := r.Read(chunk)
		if n > 0 {
			buf = append(buf, chunk[:n]...)
			derr := drainBuffer(&buf, handle)
			if derr != nil {
				return derr
			}
		}
		if err == io.EOF {
			select {
			case <-spreadDone:
				n, _ := r.Read(chunk)
				if n > 0 {
					buf = append(buf, chunk[:n]...)
				}
				return drainBuffer(&buf, handle)
			default:
			}
			time.Sleep(tailPollInterval)
			continue
		}
		if err != nil {
			return fmt.Errorf("reading events file: %w", err)
		}
	}
}
