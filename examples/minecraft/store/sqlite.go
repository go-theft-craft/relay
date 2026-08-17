// Package store records relay sessions and messages to a local SQLite
// database.
//
// It is a package rather than a file in the example beside it because several
// hundred lines of SQL and batching next to a framer would bury what a reader
// came for.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	// modernc.org/sqlite is chosen because it is pure Go. The example must
	// build without cgo, or it stops being something a reader can just run.
	_ "modernc.org/sqlite"

	"github.com/go-theft-craft/relay"
)

// Defaults chosen so the sink is useful without options and obvious with them.
const (
	defaultQueueSize     = 4096
	defaultBatchSize     = 256
	defaultFlushInterval = 250 * time.Millisecond

	// decodedSummaryLimit bounds the text written for a decoded packet. A
	// recorder that stores an unbounded rendering of every packet becomes a disk
	// problem rather than an observability one.
	decodedSummaryLimit = 512
)

const schema = `
CREATE TABLE IF NOT EXISTS sessions (
	id            INTEGER PRIMARY KEY,
	client_addr   TEXT    NOT NULL,
	upstream_addr TEXT    NOT NULL,
	port          INTEGER NOT NULL,
	opened_at     TEXT    NOT NULL,
	closed_at     TEXT
);

CREATE TABLE IF NOT EXISTS messages (
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	session_id  INTEGER NOT NULL,
	direction   TEXT    NOT NULL,
	packet_id   INTEGER,
	packet_name TEXT,
	raw         BLOB,
	decoded     TEXT,
	at          TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_chunks (
	id         INTEGER PRIMARY KEY AUTOINCREMENT,
	session_id INTEGER NOT NULL,
	direction  TEXT    NOT NULL,
	bytes      BLOB    NOT NULL,
	at         TEXT    NOT NULL
);

-- Every query a reader will write starts from a session and reads forwards.
CREATE INDEX IF NOT EXISTS messages_session ON messages(session_id, id);
CREATE INDEX IF NOT EXISTS raw_chunks_session ON raw_chunks(session_id, id);
`

// Option configures a SQLite sink.
type Option func(*SQLite)

// WithQueueSize sets how many records may be waiting to be written before the
// sink starts dropping. Only the implementation knows how deep its queue should
// be for the storage behind it, which is why this is here rather than in the
// framework.
func WithQueueSize(size int) Option {
	return func(s *SQLite) {
		if size > 0 {
			s.queueSize = size
		}
	}
}

// WithBatchSize sets how many records one transaction carries.
func WithBatchSize(size int) Option {
	return func(s *SQLite) {
		if size > 0 {
			s.batchSize = size
		}
	}
}

// WithFlushInterval sets how long a partial batch waits before it is committed
// anyway, so a quiet session's rows still land.
func WithFlushInterval(d time.Duration) Option {
	return func(s *SQLite) {
		if d > 0 {
			s.flushInterval = d
		}
	}
}

// SQLite records sessions and messages to a local database.
//
// Every write goes through a buffered channel to one writer goroutine, which
// batches into a transaction and commits on a full batch or a tick.
//
// This queue exists for the batching, not for the contract. relay.Config now
// offers a queue of its own — SinkOverflowDrop does the same counting this one
// does — but putting it in front of this sink would add a hop without removing a
// reason: records still have to arrive at one goroutine to be batched into one
// transaction, and only the implementation knows how deep its queue should be
// for the storage behind it. A consumer running this sink alone has no use for
// the core's policy; one running it beside a slower sink might, and it composes
// either way.
//
// When the queue is full the sink drops records and counts the drops. Dropping
// is the right failure for a recorder: stalling a read pump to preserve a log
// line propagates backpressure to the client and turns an observability problem
// into a relaying problem.
type SQLite struct {
	db *sql.DB

	queueSize     int
	batchSize     int
	flushInterval time.Duration

	queue chan event

	// nextID numbers sessions here rather than leaving it to the database.
	//
	// OpenSession has to return an identifier, and the only way to get one from
	// an autoincrement column is to do the insert synchronously — which would
	// put a disk write on the accept path and break the no-blocking rule on the
	// one method that is allowed to fail but still not allowed to block.
	nextID  atomic.Int64
	dropped atomic.Uint64

	stopOnce sync.Once
	stop     chan struct{}
	finished chan struct{}

	// onBatch runs after each committed batch. Tests use it to stall the writer;
	// nothing else sets it.
	onBatch func()
}

// eventKind names what a queued record is.
type eventKind uint8

const (
	eventOpen eventKind = iota
	eventMessage
	eventRawChunk
	eventClose
)

// event is one queued write. It owns every byte in it: the Sink contract lends
// its buffers for the duration of the call only.
type event struct {
	kind      eventKind
	sessionID int64
	at        time.Time

	info   relay.SessionInfo
	dir    relay.Direction
	desc   relay.Descriptor
	raw    []byte
	summar string
}

// Open creates or opens the database and starts the writer goroutine.
func Open(path string, options ...Option) (*SQLite, error) {
	s := &SQLite{
		queueSize:     defaultQueueSize,
		batchSize:     defaultBatchSize,
		flushInterval: defaultFlushInterval,
		stop:          make(chan struct{}),
		finished:      make(chan struct{}),
	}
	for _, option := range options {
		option(s)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", path, err)
	}

	// One writer goroutine means one connection is all this ever needs, and
	// holding it open avoids re-applying the pragmas on every reconnect.
	db.SetMaxOpenConns(1)

	// WAL and a relaxed sync, because this is a recorder rather than a ledger. A
	// capture sink that fsyncs on every commit will be the slowest thing in the
	// process by an order of magnitude.
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
	}
	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			_ = db.Close()

			return nil, fmt.Errorf("store: %s: %w", pragma, err)
		}
	}

	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()

		return nil, fmt.Errorf("store: apply schema: %w", err)
	}

	s.db = db
	s.queue = make(chan event, s.queueSize)

	go s.run()

	return s, nil
}

// OpenSession implements relay.Sink.
func (s *SQLite) OpenSession(_ context.Context, info relay.SessionInfo) (int64, error) {
	id := s.nextID.Add(1)

	s.enqueue(event{kind: eventOpen, sessionID: id, info: info, at: info.OpenedAt})

	return id, nil
}

// Message implements relay.Sink.
func (s *SQLite) Message(_ context.Context, id int64, record relay.MessageRecord) {
	s.enqueue(event{
		kind:      eventMessage,
		sessionID: id,
		dir:       record.Dir,
		desc:      record.Desc,
		// Raw is borrowed for the duration of the call, like everywhere else a
		// message crosses an interface, so it is copied here and not later.
		raw:    append([]byte(nil), record.Raw...),
		summar: summarise(record.Decoded),
		at:     record.At,
	})
}

// RawChunk implements relay.Sink.
//
// It is called only when relay.Config.CaptureRaw is set, and then for every
// byte crossing the client connection — below any framing, so what lands in
// raw_chunks is the conversation as it actually appeared on the wire rather
// than as the codec understood it.
func (s *SQLite) RawChunk(_ context.Context, id int64, dir relay.Direction, chunk []byte) {
	s.enqueue(event{
		kind:      eventRawChunk,
		sessionID: id,
		dir:       dir,
		raw:       append([]byte(nil), chunk...),
		at:        time.Now(),
	})
}

// CloseSession implements relay.Sink.
func (s *SQLite) CloseSession(_ context.Context, id int64) {
	s.enqueue(event{kind: eventClose, sessionID: id, at: time.Now()})
}

// Dropped reports how many records were discarded because the queue was full.
// It is exported because a silent drop count is not an observability story.
func (s *SQLite) Dropped() uint64 { return s.dropped.Load() }

// Close stops the writer, flushes whatever is queued, and closes the database.
func (s *SQLite) Close() error {
	s.stopOnce.Do(func() { close(s.stop) })
	<-s.finished

	if err := s.db.Close(); err != nil {
		return fmt.Errorf("store: close: %w", err)
	}

	return nil
}

// enqueue hands a record to the writer, or drops it. It never blocks, which is
// the whole contract.
func (s *SQLite) enqueue(e event) {
	select {
	case s.queue <- e:
	default:
		s.dropped.Add(1)
	}
}

// run is the writer goroutine: batch, commit, repeat.
func (s *SQLite) run() {
	defer close(s.finished)

	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()

	batch := make([]event, 0, s.batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}

		s.commit(batch)
		batch = batch[:0]

		if s.onBatch != nil {
			s.onBatch()
		}
	}

	for {
		select {
		case e := <-s.queue:
			batch = append(batch, e)
			if len(batch) >= s.batchSize {
				flush()
			}

		case <-ticker.C:
			flush()

		case <-s.stop:
			// Drain what is already queued before going. A batch that never
			// filled is exactly the case a shutdown has to rescue, and losing it
			// would mean the last thing that happened is the thing never
			// recorded.
			for {
				select {
				case e := <-s.queue:
					batch = append(batch, e)
					if len(batch) >= s.batchSize {
						flush()
					}
				default:
					flush()

					return
				}
			}
		}
	}
}

// commit writes one batch in a single transaction.
//
// A failed batch is counted as dropped rather than retried. This is a recorder:
// blocking the queue to retry a write would eventually stall the sessions it is
// recording, which is the failure it exists to avoid.
func (s *SQLite) commit(batch []event) {
	tx, err := s.db.Begin()
	if err != nil {
		s.dropped.Add(uint64(len(batch)))

		return
	}

	for _, e := range batch {
		if err := writeEvent(tx, e); err != nil {
			s.dropped.Add(1)
		}
	}

	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		s.dropped.Add(uint64(len(batch)))
	}
}

func writeEvent(tx *sql.Tx, e event) error {
	switch e.kind {
	case eventOpen:
		_, err := tx.Exec(
			`INSERT INTO sessions (id, client_addr, upstream_addr, port, opened_at)
			 VALUES (?, ?, ?, ?, ?)`,
			e.sessionID, e.info.ClientAddr, e.info.UpstreamAddr, e.info.Port, stamp(e.info.OpenedAt),
		)

		return err

	case eventMessage:
		_, err := tx.Exec(
			`INSERT INTO messages (session_id, direction, packet_id, packet_name, raw, decoded, at)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			e.sessionID, e.dir.String(), e.desc.ID, e.desc.Name, e.raw, e.summar, stamp(e.at),
		)

		return err

	case eventRawChunk:
		_, err := tx.Exec(
			`INSERT INTO raw_chunks (session_id, direction, bytes, at) VALUES (?, ?, ?, ?)`,
			e.sessionID, e.dir.String(), e.raw, stamp(e.at),
		)

		return err

	case eventClose:
		_, err := tx.Exec(`UPDATE sessions SET closed_at = ? WHERE id = ?`, stamp(e.at), e.sessionID)

		return err

	default:
		return errors.New("store: unknown event kind")
	}
}

// stamp renders a time the way every row in this schema renders one, so a
// reader can sort on the column as text.
func stamp(t time.Time) string {
	if t.IsZero() {
		t = time.Now()
	}

	return t.UTC().Format(time.RFC3339Nano)
}

// summarise renders a decoded packet for the text column, bounded.
//
// JSON first, and %+v only as a fallback, because a decoded packet is usually a
// struct holding a pointer to a generated value — and %+v renders that pointer
// as an address. A capture column full of 0xc000c0ffee is worse than an empty
// one, because it looks like it worked.
func summarise(decoded any) string {
	if decoded == nil {
		return ""
	}

	text := fmt.Sprintf("%+v", decoded)
	if encoded, err := json.Marshal(decoded); err == nil {
		text = string(encoded)
	}

	if len(text) > decodedSummaryLimit {
		var b strings.Builder

		b.WriteString(text[:decodedSummaryLimit])
		b.WriteString("…")

		return b.String()
	}

	return text
}
