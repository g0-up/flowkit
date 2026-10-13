package chaos

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/g0-up/flowkit"
	"github.com/g0-up/flowkit/internal/testhooks"
)

// The environment of a worker process.
const (
	envPoint   = "FLOWKIT_CHAOS_POINT"
	envSchema  = "FLOWKIT_CHAOS_SCHEMA"
	envSink    = "FLOWKIT_CHAOS_SINK"
	envWorkers = "FLOWKIT_CHAOS_WORKERS"
)

const (
	// reachTimeout bounds how long a worker process may take to reach its
	// crash point.
	reachTimeout = time.Minute
	// workerLifetime bounds a worker process nobody kills.
	workerLifetime = 3 * time.Minute
	// atPrefix starts the line a worker process prints at its crash point.
	atPrefix = "AT "
	// raceReport starts each race the race detector reports on stderr.
	raceReport = "WARNING: DATA RACE"
)

// TestChaosWorker is the process killAt starts: a worker-only Engine whose
// crash point, once reached, prints "AT <point>" and blocks every goroutine
// reaching it until the parent kills the process. It skips unless started
// by killAt.
func TestChaosWorker(t *testing.T) {
	point := os.Getenv(envPoint)
	if point == "" {
		t.Skip("runs only as a process the chaos tests start and kill")
	}
	// A worker outliving its parent, whose end closes stdin, or never
	// killed, exits by itself.
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		os.Exit(2)
	}()
	time.AfterFunc(workerLifetime, func() {
		fmt.Fprintf(os.Stderr, "chaos worker: not killed within %s\n", workerLifetime)
		os.Exit(3)
	})
	workers, err := strconv.Atoi(os.Getenv(envWorkers))
	if err != nil {
		t.Fatalf("%s: %v", envWorkers, err)
	}

	block := make(chan struct{})
	var once sync.Once
	testhooks.Point = func(name string) {
		if name != point {
			return
		}
		once.Do(func() { fmt.Printf("%s%s\n", atPrefix, name) })
		<-block
	}
	// The sink writes on a pool of its own, as an external system would.
	sink := NewSink(newPool(t, 2), os.Getenv(envSink))
	pool := newPool(t, int32(workers)+4)
	e := newEngine(t, pool, os.Getenv(envSchema), sink, os.Stderr, workers, flowkit.RoleWorker)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("start engine: %v", err)
	}
	<-block
}

// killAt starts a worker process of workers job slots on the fixture's
// schemas, waits until it reaches point and kills it with SIGKILL. It fails
// the test when the process exits or takes longer than reachTimeout first.
func (f *fixture) killAt(point string, workers int) {
	f.t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestChaosWorker$", "-test.count=1")
	cmd.Env = append(os.Environ(),
		envPoint+"="+point, envSchema+"="+f.schema, envSink+"="+f.sinkSchema, envWorkers+"="+strconv.Itoa(workers))
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		f.t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		f.t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		f.t.Fatalf("start worker process: %v", err)
	}
	reached, scanned := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(scanned)
		sc := bufio.NewScanner(stdout)
		seen := false
		for sc.Scan() {
			if !seen && sc.Text() == atPrefix+point {
				seen = true
				close(reached)
			}
		}
	}()
	timer := time.NewTimer(reachTimeout)
	defer timer.Stop()
	failure := ""
	select {
	case <-reached:
	case <-scanned:
		failure = "exited before reaching " + point
	case <-timer.C:
		failure = fmt.Sprintf("did not reach %s within %s", point, reachTimeout)
	}
	killErr := cmd.Process.Kill()
	<-scanned
	_ = cmd.Wait()
	_ = stdin.Close()
	switch {
	case failure != "":
		f.t.Fatalf("worker process %s; its stderr:\n%s", failure, stderr.String())
	case killErr != nil:
		f.t.Fatalf("kill worker process at %s: %v", point, killErr)
	case cmd.ProcessState.String() != "signal: killed":
		f.t.Fatalf("worker process at %s ended with %s, want SIGKILL; its stderr:\n%s", point, cmd.ProcessState, stderr.String())
	// The race detector reports a race as it happens, but a killed process
	// never exits with the detector's status, so its report is the only
	// trace of the race.
	case strings.Contains(stderr.String(), raceReport):
		f.t.Fatalf("worker process killed at %s reported a data race; its stderr:\n%s", point, stderr.String())
	}
}
