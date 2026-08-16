package store

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-theft-craft/relay"
)

// openTemp opens a sink on a temporary database and returns it with the path,
// so a test can read the file back through its own connection after Close.
func openTemp(t *testing.T, options ...Option) (*SQLite, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "relay.db")

	s, err := Open(path, options...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	return s, path
}

// readback opens the written database independently of the sink, which is the
// only honest way to assert what actually reached the disk.
func readback(t *testing.T, path string) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("reopen %s: %v", path, err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return db
}

func info(client string) relay.SessionInfo {
	return relay.SessionInfo{
		ClientAddr:   client,
		UpstreamAddr: "10.0.0.1:25565",
		Port:         25565,
		OpenedAt:     time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC),
	}
}

func TestOpenSessionRecordsTheRow(t *testing.T) {
	s, path := openTemp(t)

	id, err := s.OpenSession(context.Background(), info("1.2.3.4:5000"))
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	if id == 0 {
		t.Fatal("OpenSession returned 0, want an identifier")
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var (
		client   string
		upstream string
		port     int
		opened   string
	)
	row := query(t, readback(t, path), `SELECT client_addr, upstream_addr, port, opened_at FROM sessions WHERE id = ?`, id)
	if err := row.Scan(&client, &upstream, &port, &opened); err != nil {
		t.Fatalf("scan the session row: %v", err)
	}

	if client != "1.2.3.4:5000" || upstream != "10.0.0.1:25565" || port != 25565 {
		t.Fatalf("session row = (%q, %q, %d), want the values it was given", client, upstream, port)
	}
	if opened == "" {
		t.Fatal("opened_at was not recorded")
	}
}

func TestMessagesLandInOrder(t *testing.T) {
	s, path := openTemp(t)

	id, err := s.OpenSession(context.Background(), info("1.2.3.4:5000"))
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	want := []string{"first", "second", "third"}
	for i, text := range want {
		s.Message(context.Background(), id, relay.MessageRecord{
			Dir:  relay.ToServer,
			Desc: relay.Descriptor{ID: int32(i), Name: text},
			Raw:  []byte(text),
			At:   time.Now(),
		})
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	rows, err := readback(t, path).Query(`SELECT direction, packet_id, packet_name, raw FROM messages WHERE session_id = ? ORDER BY id`, id)
	if err != nil {
		t.Fatalf("query messages: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var got []string
	for rows.Next() {
		var (
			dir  string
			pid  int32
			name string
			raw  []byte
		)
		if err := rows.Scan(&dir, &pid, &name, &raw); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if dir != "to_server" {
			t.Fatalf("direction = %q, want to_server", dir)
		}
		if name != string(raw) {
			t.Fatalf("row mismatch: name %q, raw %q", name, raw)
		}

		got = append(got, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("recorded %d messages, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("message %d = %q, want %q — rows must land in submission order", i, got[i], want[i])
		}
	}
}

// TestMessageDoesNotBlock is the contract the core depends on and the one a
// sink is most likely to break. The writer is stalled and far more messages
// than the queue holds are submitted; the submitting goroutine must still
// return promptly, dropping rather than waiting.
func TestMessageDoesNotBlock(t *testing.T) {
	const queueSize = 8

	release := make(chan struct{})
	stalled := make(chan struct{})

	var once sync.Once

	s, _ := openTemp(t, WithQueueSize(queueSize), WithBatchSize(1), WithFlushInterval(time.Hour))
	s.onBatch = func() {
		once.Do(func() { close(stalled) })
		<-release
	}

	id, err := s.OpenSession(context.Background(), info("1.2.3.4:5000"))
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	// Wait for the writer to be wedged inside a batch before piling on.
	select {
	case <-stalled:
	case <-time.After(5 * time.Second):
		t.Fatal("the writer never reached a batch")
	}

	done := make(chan struct{})

	go func() {
		defer close(done)

		for i := range queueSize * 50 {
			s.Message(context.Background(), id, relay.MessageRecord{
				Dir:  relay.ToServer,
				Desc: relay.Descriptor{ID: int32(i)},
				Raw:  []byte("payload"),
				At:   time.Now(),
			})
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("Message blocked; a sink that blocks stalls a read pump and, through backpressure, its peer")
	}

	if s.Dropped() == 0 {
		t.Fatal("nothing was reported dropped, so the queue cannot have been the thing that absorbed it")
	}

	close(release)
}

// TestMessageCopiesTheBorrowedBuffer pins the ownership rule. MessageRecord.Raw
// is lent for the duration of the call, and the relay reuses what is behind it.
func TestMessageCopiesTheBorrowedBuffer(t *testing.T) {
	s, path := openTemp(t)

	id, err := s.OpenSession(context.Background(), info("1.2.3.4:5000"))
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	raw := []byte("original")
	s.Message(context.Background(), id, relay.MessageRecord{
		Dir:  relay.ToClient,
		Desc: relay.Descriptor{ID: 1, Name: "packet"},
		Raw:  raw,
		At:   time.Now(),
	})

	// The caller reuses its buffer the instant the call returns, exactly as the
	// relay's message pool does.
	copy(raw, []byte("OVERWROTE"))

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var stored []byte
	if err := query(t, readback(t, path), `SELECT raw FROM messages WHERE session_id = ?`, id).Scan(&stored); err != nil {
		t.Fatalf("scan: %v", err)
	}

	if !bytes.Equal(stored, []byte("original")) {
		t.Fatalf("stored %q, want original — the sink kept the caller's buffer instead of copying it", stored)
	}
}

// TestCloseFlushesAPartialBatch is the case a timer-driven flush would lose:
// the batch never fills, and the process is going away.
func TestCloseFlushesAPartialBatch(t *testing.T) {
	s, path := openTemp(t, WithBatchSize(1000), WithFlushInterval(time.Hour))

	id, err := s.OpenSession(context.Background(), info("1.2.3.4:5000"))
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	s.Message(context.Background(), id, relay.MessageRecord{
		Dir: relay.ToServer,
		Raw: []byte("only one"),
		At:  time.Now(),
	})
	s.CloseSession(context.Background(), id)

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var messages int
	if err := query(t, readback(t, path), `SELECT count(*) FROM messages WHERE session_id = ?`, id).Scan(&messages); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if messages != 1 {
		t.Fatalf("recorded %d messages, want 1 — a batch that never filled was lost on close", messages)
	}

	var closedAt sql.NullString
	if err := query(t, readback(t, path), `SELECT closed_at FROM sessions WHERE id = ?`, id).Scan(&closedAt); err != nil {
		t.Fatalf("scan closed_at: %v", err)
	}
	if !closedAt.Valid || closedAt.String == "" {
		t.Fatal("CloseSession did not reach the row")
	}
}

// TestConcurrentSessionsDoNotCross is what the session_id column is for.
func TestConcurrentSessionsDoNotCross(t *testing.T) {
	const (
		sessions = 4
		messages = 25
	)

	s, path := openTemp(t)

	ids := make([]int64, sessions)
	for i := range ids {
		id, err := s.OpenSession(context.Background(), info("client"))
		if err != nil {
			t.Fatalf("OpenSession: %v", err)
		}

		ids[i] = id
	}

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for range messages {
				s.Message(context.Background(), id, relay.MessageRecord{
					Dir:  relay.ToServer,
					Desc: relay.Descriptor{Name: "packet"},
					Raw:  []byte("payload"),
					At:   time.Now(),
				})
			}
		}()
	}
	wg.Wait()

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if dropped := s.Dropped(); dropped != 0 {
		t.Fatalf("%d records were dropped; the default queue should have absorbed this", dropped)
	}

	for _, id := range ids {
		var count int
		if err := query(t, readback(t, path), `SELECT count(*) FROM messages WHERE session_id = ?`, id).Scan(&count); err != nil {
			t.Fatalf("count for session %d: %v", id, err)
		}
		if count != messages {
			t.Fatalf("session %d recorded %d messages, want %d", id, count, messages)
		}
	}
}

// query runs a single-row query against the still-open handle. Close shuts the
// writer down but the tests read through the same *sql.DB before it is gone.
func query(t *testing.T, db *sql.DB, statement string, args ...any) *sql.Row {
	t.Helper()

	return db.QueryRow(statement, args...)
}
