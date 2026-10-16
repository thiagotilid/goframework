package goframework

import (
	"bytes"
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// fakeSource is a messageSource driven by the test: no broker involved.
type fakeSource struct {
	queue chan *kafka.Message

	mu     sync.Mutex
	events []string
	closed chan struct{}
}

func newFakeSource() *fakeSource {
	return &fakeSource{queue: make(chan *kafka.Message, 100), closed: make(chan struct{})}
}

func (f *fakeSource) SubscribeTopics([]string, kafka.RebalanceCb) error { return nil }

func (f *fakeSource) ReadMessage(timeout time.Duration) (*kafka.Message, error) {
	select {
	case m := <-f.queue:
		return m, nil
	case <-time.After(timeout):
		return nil, kafka.NewError(kafka.ErrTimedOut, "Local: Timed out", false)
	}
}

func (f *fakeSource) CommitMessage(m *kafka.Message) ([]kafka.TopicPartition, error) {
	f.record("commit:" + string(m.Key))
	return nil, nil
}

func (f *fakeSource) Close() error {
	f.record("close")
	close(f.closed)
	return nil
}

func (f *fakeSource) record(e string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, e)
}

func (f *fakeSource) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.events...)
}

func (f *fakeSource) indexOf(event string) int {
	for i, e := range f.snapshot() {
		if e == event {
			return i
		}
	}
	return -1
}

func (f *fakeSource) lastIs(event string) bool {
	ev := f.snapshot()
	return len(ev) > 0 && ev[len(ev)-1] == event
}

func tenantMessage(key string) *kafka.Message {
	topic := "test-topic"
	return &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Key:            []byte(key),
		Value:          []byte("{}"),
		Headers:        []kafka.Header{{Key: XTENANTID, Value: []byte(testTenant)}},
	}
}

// useFakeKafka swaps the Kafka seams for the duration of the test.
func useFakeKafka(t *testing.T) (src *fakeSource, opened *int, dlq *[]string) {
	t.Helper()
	src = newFakeSource()
	opened = new(int)
	dlq = new([]string)
	var mu sync.Mutex

	origNew, origCreate, origDlq := newMessageSource, createTopic, sendToDlq
	newMessageSource = func(*kafka.ConfigMap) (messageSource, error) {
		mu.Lock()
		*opened++
		mu.Unlock()
		return src, nil
	}
	createTopic = func(context.Context, *kafka.ConfigMap, *TopicConfiguration) {}
	sendToDlq = func(_ context.Context, _ *kafka.ConfigMap, msg *kafka.Message, _ error, _ []byte) {
		mu.Lock()
		*dlq = append(*dlq, string(msg.Key))
		mu.Unlock()
	}
	t.Cleanup(func() { newMessageSource, createTopic, sendToDlq = origNew, origCreate, origDlq })
	return src, opened, dlq
}

func newTestKafka() *GoKafka {
	k := NewKafkaConfigMap("localhost:9092", "test-group", "", "", "", "")
	k.lc = newLifecycle()
	return k
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func drained(t *testing.T, lc *lifecycle) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return lc.wait(ctx)
}

// The core guarantee: a message being processed when the signal arrives is finished,
// committed, and only then does the consumer leave the group. The handler keeps receiving
// a *ConsumerContext with the tenant of its message.
func TestConsumerFinishesInFlightMessageThenCommitsAndCloses(t *testing.T) {
	src, _, _ := useFakeKafka(t)
	k := newTestKafka()

	started := make(chan struct{})
	release := make(chan struct{})
	tenants := make(chan string, 2)
	k.Consumer("test-topic", func(ctx *ConsumerContext) {
		close(started)
		<-release
		tenants <- GetContextHeader(ctx, XTENANTID, TTENANTID)
		tenants <- GetContextHeader(ToContext(ctx), XTENANTID)
	})

	src.queue <- tenantMessage("m1")
	<-started

	k.lc.begin()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if k.lc.wait(ctx) {
		t.Fatal("shutdown finished while a message was still being processed")
	}

	close(release)
	if !drained(t, k.lc) {
		t.Fatal("consumer did not finish after the handler returned")
	}

	if got := <-tenants; got != testTenant {
		t.Fatalf("tenant seen by the handler after the signal = %q, want %q", got, testTenant)
	}
	if got := <-tenants; got != testTenant {
		t.Fatalf("tenant via ToContext after the signal = %q, want %q", got, testTenant)
	}
	if c, cl := src.indexOf("commit:m1"), src.indexOf("close"); c < 0 || cl < 0 || c > cl {
		t.Fatalf("events = %v, want the commit before close", src.snapshot())
	}
}

func TestConsumerWithSettingsPanicStillGoesToDlqDuringShutdown(t *testing.T) {
	src, _, dlq := useFakeKafka(t)
	k := newTestKafka()

	started := make(chan struct{})
	release := make(chan struct{})
	k.ConsumerWithSettings("test-topic", func(ctx *ConsumerContext) {
		close(started)
		<-release
		panic("boom")
	}, ConsumerSettings{AutoOffsetReset: "earliest", Retries: 1})

	src.queue <- tenantMessage("bad")
	<-started
	k.lc.begin()
	close(release)

	if !drained(t, k.lc) {
		t.Fatal("consumer did not finish")
	}
	if len(*dlq) != 1 || (*dlq)[0] != "bad" {
		t.Fatalf("dlq = %v, want [bad]", *dlq)
	}
	if src.indexOf("commit:bad") < 0 || !src.lastIs("close") {
		t.Fatalf("events = %v, want the failed message committed and then close", src.snapshot())
	}
}

func TestConsumerRegisteredAfterShutdownDoesNotStart(t *testing.T) {
	_, opened, _ := useFakeKafka(t)
	k := newTestKafka()

	k.lc.begin()
	k.Consumer("late", func(*ConsumerContext) {})
	k.ConsumerWithSettings("late", func(*ConsumerContext) {}, ConsumerSettings{})
	k.ConsumerMultiRoutine("late", func(*ConsumerContext) {}, ConsumerMultiRoutineSettings{Routines: 1})
	k.ConsumerWithWorker("late", func(*ConsumerContext) {}, ConsumerMultiRoutineSettings{Routines: 1})

	if !drained(t, k.lc) {
		t.Fatal("nothing should be running")
	}
	if *opened != 0 {
		t.Fatalf("%d consumers were opened after the shutdown began, want 0", *opened)
	}
}

func TestMultiRoutineWaitsForInFlightMessagesBeforeClose(t *testing.T) {
	src, _, _ := useFakeKafka(t)
	k := newTestKafka()

	started := make(chan struct{})
	release := make(chan struct{})
	tenant := make(chan string, 1)
	k.ConsumerMultiRoutine("test-topic", func(ctx *ConsumerContext) {
		close(started)
		<-release
		tenant <- GetContextHeader(ctx, XTENANTID)
	}, ConsumerMultiRoutineSettings{Routines: 2, AutoOffsetReset: "earliest", Retries: 1})

	src.queue <- tenantMessage("m1")
	<-started
	k.lc.begin()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if k.lc.wait(ctx) {
		t.Fatal("shutdown finished while a message was still being processed")
	}

	close(release)
	if !drained(t, k.lc) {
		t.Fatal("consumer did not finish")
	}
	if got := <-tenant; got != testTenant {
		t.Fatalf("tenant = %q, want %q", got, testTenant)
	}
	if c, cl := src.indexOf("commit:m1"), src.indexOf("close"); c < 0 || c > cl {
		t.Fatalf("events = %v, want the commit before close", src.snapshot())
	}
	if !src.lastIs("close") {
		t.Fatalf("events = %v, close must be last", src.snapshot())
	}
}

// Regression: the worker used to swallow a message from the channel after each one it
// processed, so some messages were never handled.
func TestWorkerHandlesEveryMessageAndClosesLast(t *testing.T) {
	src, _, _ := useFakeKafka(t)
	k := newTestKafka()

	var mu sync.Mutex
	handled := map[string]int{}
	k.ConsumerWithWorker("test-topic", func(ctx *ConsumerContext) {
		mu.Lock()
		handled[string(ctx.Msg.Key)]++
		mu.Unlock()
	}, ConsumerMultiRoutineSettings{Routines: 2, AutoOffsetReset: "earliest", Retries: 1})

	keys := []string{"m1", "m2", "m3", "m4", "m5"}
	for _, key := range keys {
		src.queue <- tenantMessage(key)
	}

	waitFor(t, "all messages handled", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(handled) == len(keys)
	})

	k.lc.begin()
	if !drained(t, k.lc) {
		t.Fatal("worker consumer did not finish")
	}

	mu.Lock()
	defer mu.Unlock()
	for _, key := range keys {
		if handled[key] != 1 {
			t.Fatalf("message %s handled %d times, want exactly 1 (all: %v)", key, handled[key], handled)
		}
	}
	if !src.lastIs("close") {
		t.Fatalf("events = %v, close must be last", src.snapshot())
	}
}

// A GoKafka not created through RegisterKafka has no lifecycle and keeps the old behavior:
// the consumer simply runs.
func TestGoKafkaWithoutLifecycleKeepsConsuming(t *testing.T) {
	src, _, _ := useFakeKafka(t)
	k := NewKafkaConfigMap("localhost:9092", "test-group", "", "", "", "")

	got := make(chan string, 1)
	k.Consumer("test-topic", func(ctx *ConsumerContext) {
		got <- GetContextHeader(ctx, XTENANTID)
	})

	src.queue <- tenantMessage("m1")

	select {
	case tenant := <-got:
		if tenant != testTenant {
			t.Fatalf("tenant = %q, want %q", tenant, testTenant)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("message not handled")
	}
}

// Without messages the poll times out every 500ms. That is normal and must not be logged
// as an error.
func TestPollTimeoutIsNotLogged(t *testing.T) {
	var buf syncBuffer
	log.SetOutput(&buf)
	defer log.SetOutput(log.Writer())

	src := newFakeSource()
	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()
	consumeLoop(ctx, src, func(*kafka.Message) {})

	if out := buf.String(); out != "" {
		t.Fatalf("poll timeouts were logged: %q", out)
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
