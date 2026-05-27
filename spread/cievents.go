// This file defines spread's CI event stream: an opt-in JSONL log of run and
// task lifecycle transitions, intended for consumption by CI systems that
// drive per-task reporting (e.g. one GitHub Check Run per spread task)
// without parsing spread's human-readable stdout. The stream is written to a
// file specified by the -ci-events flag and is additive — it does not affect
// spread's existing stdout or default behaviour.
//
// The event shapes here form a public contract with downstream consumers.
// Field names, types, and semantics are part of that contract; changes that
// affect consumers should bump [ciEventsSchemaVersion].

package spread

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type ciEventsWriter struct {
	mu  sync.Mutex
	now func() time.Time
	out io.WriteCloser
}

// ciPhase is the value reported in task_phase's phase field and in
// task_finished's failed_phase field.
type ciPhase string

// ciStatus is the outcome reported in task_finished's status field.
type ciStatus string

type runFinishedEvent struct {
	Aborted int    `json:"aborted"`
	Event   string `json:"event"`
	Failed  int    `json:"failed"`
	Passed  int    `json:"passed"`
	TS      string `json:"ts"`
}

type runStartedEvent struct {
	Event         string `json:"event"`
	SchemaVersion int    `json:"schema_version"`
	Seed          int64  `json:"seed"`
	TaskCount     int    `json:"task_count"`
	TS            string `json:"ts"`
}

type taskFinishedEvent struct {
	Attempt       int      `json:"attempt,omitempty"`
	AttemptsTotal int      `json:"attempts_total,omitempty"`
	DurationMS    int64    `json:"duration_ms,omitempty"`
	Event         string   `json:"event"`
	FailedPhase   ciPhase  `json:"failed_phase,omitempty"`
	ID            string   `json:"id"`
	Status        ciStatus `json:"status"`
	TS            string   `json:"ts"`
}

type taskPhaseEvent struct {
	Event string  `json:"event"`
	ID    string  `json:"id"`
	Phase ciPhase `json:"phase"`
	TS    string  `json:"ts"`
}

type taskStartedEvent struct {
	Event string `json:"event"`
	ID    string `json:"id"`
	TS    string `json:"ts"`
}

// ciEventsSchemaVersion is the version of the event stream contract.
// Bump on additions with semantic meaning or changes to existing field shapes.
const ciEventsSchemaVersion = 1

const (
	phaseExecute ciPhase = "execute"
	phasePrepare ciPhase = "prepare"
	phaseRestore ciPhase = "restore"
)

const (
	// statusAborted indicates the task never ran — surrounding suite, backend,
	// or project prepare failed, or the run stopped before this task was
	// reached. No failed_phase is set.
	statusAborted ciStatus = "aborted"

	// statusFailed indicates a task-level phase failed; the accompanying
	// failed_phase field identifies which (prepare, execute, restore).
	statusFailed ciStatus = "failed"

	// statusPassed indicates the task's execute phase completed successfully.
	statusPassed ciStatus = "passed"
)

func (w *ciEventsWriter) Close() error {
	if w == nil {
		return nil
	}
	return w.out.Close()
}

func (w *ciEventsWriter) emit(v any) {
	data, err := json.Marshal(v)
	if err != nil {
		printf("Cannot marshal spread event: %v", err)
		return
	}
	data = append(data, '\n')
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err = w.out.Write(data)
	if err != nil {
		printf("Cannot write spread event: %v", err)
	}
}

func (w *ciEventsWriter) EmitRunFinished(passed, failed, aborted int) {
	if w == nil {
		return
	}
	w.emit(runFinishedEvent{
		Aborted: aborted,
		Event:   "run_finished",
		Failed:  failed,
		Passed:  passed,
		TS:      w.nowTS(),
	})
}

func (w *ciEventsWriter) EmitRunStarted(seed int64, taskCount int) {
	if w == nil {
		return
	}
	w.emit(runStartedEvent{
		Event:         "run_started",
		SchemaVersion: ciEventsSchemaVersion,
		Seed:          seed,
		TaskCount:     taskCount,
		TS:            w.nowTS(),
	})
}

func (w *ciEventsWriter) EmitTaskFinished(
	id string,
	status ciStatus,
	failedPhase ciPhase,
	attempt, attemptsTotal int,
	durationMS int64,
) {
	if w == nil {
		return
	}
	w.emit(taskFinishedEvent{
		Attempt:       attempt,
		AttemptsTotal: attemptsTotal,
		DurationMS:    durationMS,
		Event:         "task_finished",
		FailedPhase:   failedPhase,
		ID:            id,
		Status:        status,
		TS:            w.nowTS(),
	})
}

func (w *ciEventsWriter) EmitTaskPhase(id string, phase ciPhase) {
	if w == nil {
		return
	}
	w.emit(taskPhaseEvent{
		Event: "task_phase",
		ID:    id,
		Phase: phase,
		TS:    w.nowTS(),
	})
}

func (w *ciEventsWriter) EmitTaskStarted(id string) {
	if w == nil {
		return
	}
	w.emit(taskStartedEvent{
		Event: "task_started",
		ID:    id,
		TS:    w.nowTS(),
	})
}

func makeCIEventsWriter(path string) (*ciEventsWriter, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("cannot open events file %s: %w", path, err)
	}
	return &ciEventsWriter{now: time.Now, out: f}, nil
}

func (w *ciEventsWriter) nowTS() string {
	return w.now().UTC().Format(time.RFC3339)
}

func (p ciPhase) String() string {
	return string(p)
}

func (s ciStatus) String() string {
	return string(s)
}
