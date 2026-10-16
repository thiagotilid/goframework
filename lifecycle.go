package goframework

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultShutdownTimeout = 20 * time.Second
	otelFlushTimeout       = 3 * time.Second
)

// lifecycle coordinates the graceful shutdown of the long-running parts of the framework
// (today, the Kafka consumers). The HTTP server is drained by Start().
//
// Its context only controls the consumer read loops. It is never handed to message
// handlers, so a handler is not cancelled by the shutdown signal and the context type the
// handler receives (*ConsumerContext) is untouched. The tenant travels through that type.
//
// All methods are safe on a nil receiver: a nil lifecycle never stops anything, which is
// the behavior the consumers had before graceful shutdown existed.
type lifecycle struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup

	shuttingDown atomic.Bool
}

func newLifecycle() *lifecycle {
	ctx, cancel := context.WithCancel(context.Background())
	return &lifecycle{ctx: ctx, cancel: cancel}
}

// Context is cancelled when the shutdown begins.
func (l *lifecycle) Context() context.Context {
	if l == nil {
		return context.Background()
	}
	return l.ctx
}

// track registers a long-running worker. It returns false once the shutdown has begun, so
// workers registered late (for example from a boot goroutine) are never started.
// Every successful track must be paired with a done.
func (l *lifecycle) track() bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return false
	}
	l.wg.Add(1)
	return true
}

func (l *lifecycle) done() {
	if l == nil {
		return
	}
	l.wg.Done()
}

// begin marks the shutdown as started: no new worker is accepted, readiness turns
// unhealthy and the workers are told to stop.
func (l *lifecycle) begin() {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	l.shuttingDown.Store(true)
	l.cancel()
}

func (l *lifecycle) isShuttingDown() bool {
	return l != nil && l.shuttingDown.Load()
}

// wait blocks until every tracked worker is done or ctx expires. It reports whether all
// workers finished.
func (l *lifecycle) wait(ctx context.Context) bool {
	if l == nil {
		return true
	}
	finished := make(chan struct{})
	go func() {
		l.wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
		return true
	case <-ctx.Done():
		return false
	}
}
