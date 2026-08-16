# relay Proxy Framework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `github.com/go-theft-craft/relay`, a protocol-agnostic TCP proxy framework, plus a worked Minecraft example in a second module.

**Architecture:** A dependency-free core module accepts connections on many ports, resolves an upstream per connection through a lazily-probed health cache and a pluggable selector, then relays framed messages with two goroutines per session. Consumers supply a `Framer` (required) and optionally a `Codec`, `Prober`, `Sink`, and hooks. A separate `examples/` module builds a Minecraft proxy on top, which is where every third-party dependency lives.

**Tech Stack:** Go 1.26, devbox, Task, golangci-lint. Example module only: `github.com/go-theft-craft/minecraft-protocol`, `modernc.org/sqlite`.

**Spec:** `docs/superpowers/specs/2026-08-16-relay-proxy-framework-design.md` in the `minecraft-protocol` repository. Move it into `docs/` in the new repository as part of Task 1.

## Global Constraints

- Work in a new repository at `/home/ocharnyshevich/pet.projects/go-theft-craft/relay`. Nothing in this plan modifies `minecraft-protocol`.
- Module path `github.com/go-theft-craft/relay`. Go version `1.26.6`.
- **The core module's `go.mod` must contain no `require` and no `replace` line.** Task 1 adds a CI check that fails the build otherwise. Every later task must keep it passing.
- Run every command as `devbox run -- task <name>`. Never call `go` directly.
- Tests run with `-race`. A task is not done until `devbox run -- task test` is green.
- Never add a `Co-Authored-By` or `Claude-Session` trailer to a commit message.
- This repository is public. Do not name the private proxy project, its protocol, or its repository directory anywhere in source, comments, docs, or commit messages.
- Exported identifiers get doc comments. Match the prose style of `minecraft-protocol`: say why a thing exists, not what the next line does.

## File Structure

Core module, `github.com/go-theft-craft/relay`:

| File | Responsibility |
| --- | --- |
| `message.go` | `Direction`, `Action`, `Descriptor`, `Message` and its mutation API |
| `errors.go` | sentinel errors |
| `framer.go` | `Framer`, `Codec`, `Prober` interfaces and the default TCP-dial prober |
| `hook.go` | `Hook`, `HookFunc`, `PreFrame`, `PreFrameResult`, chain execution |
| `sink.go` | `Sink`, `SessionInfo`, `MessageRecord`, no-op sink |
| `conduit.go` | per-direction byte layer, `Transform`, mid-stream swaps |
| `session.go` | session lifecycle, read pumps, writer locks, injection |
| `registry.go` | live session set, per-upstream counts, snapshots, drain |
| `health.go` | probe cache: TTL, single-flight, concurrent fan-out |
| `selector.go` | `Selector`, `Upstream`, four built-in selectors |
| `config.go` | `Config`, validation, defaults |
| `relay.go` | `Proxy`, listeners, accept loop, upstream resolution, `Run` |
| `typed/typed.go` | generic wrappers over the untyped core |
| `relaytest/framer.go` | `Framer` conformance harness for consumers |

Example module, `examples/`:

| File | Responsibility |
| --- | --- |
| `cipher/proxy.go` | `relay.Transform`'s worked consumer: an AES-CTR mid-stream swap |
| `minecraft/framer.go` | `relay.Framer` over Java frame boundaries |
| `minecraft/codec.go` | `relay.Codec` over decoded packets, per-direction state |
| `minecraft/prober.go` | `relay.Prober` performing a real status ping |
| `minecraft/store/sqlite.go` | async batched SQLite sink |
| `minecraft/main.go` | flags, wiring, graceful stop |
| `minecraft/proxy_test.go` | end-to-end through a stub upstream |

---

### Task 1: Repository bootstrap and the zero-dependency gate

**Files:**
- Create: `go.mod`, `devbox.json`, `Taskfile.yml`, `.gitignore`, `LICENSE`, `README.md`, `doc.go`, `docs/2026-08-16-relay-proxy-framework-design.md`, `.github/workflows/ci.yml`

**Interfaces:**
- Produces: a buildable empty module and the `task deps:check` gate every later task must keep green.

- [ ] **Step 1: Create the repository and module**

```bash
mkdir -p /home/ocharnyshevich/pet.projects/go-theft-craft/relay
cd /home/ocharnyshevich/pet.projects/go-theft-craft/relay
git init
printf 'module github.com/go-theft-craft/relay\n\ngo 1.26.6\n' > go.mod
```

- [ ] **Step 2: Copy the toolchain files**

Copy `devbox.json` from `../minecraft-protocol/devbox.json` verbatim, then delete the `"nodejs": "24"` entry — the core module has no Node interop.

```bash
cp ../minecraft-protocol/devbox.json ./devbox.json
cp ../minecraft-protocol/LICENSE ./LICENSE
```

Edit `devbox.json` to remove the `nodejs` line.

- [ ] **Step 3: Write the Taskfile**

Create `Taskfile.yml`:

```yaml
version: "3"

vars:
  MODULE:
    sh: go list -m

tasks:
  deps:
    desc: Download and normalize Go dependencies
    sources: [go.mod]
    cmds:
      - go mod tidy

  deps:check:
    desc: Assert the core module depends on nothing outside the standard library
    cmds:
      - cmd: '! grep -Eq "^[[:space:]]*(require|replace)[[:space:]]" go.mod'
        platforms: [linux, darwin]

  fmt:
    desc: Format Go source
    cmds:
      - gci write -s standard -s default -s "prefix(github.com/go-theft-craft)" -s "prefix({{.MODULE}})" .
      - gofumpt -w .

  fmt:check:
    desc: Check Go formatting without changing files
    cmds:
      - |
        formatting_diff="$(golangci-lint fmt --diff)"
        if [ -n "$formatting_diff" ]; then
          printf '%s\n' "$formatting_diff"
          exit 1
        fi

  lint:
    desc: Run static analysis
    deps: [fmt:check]
    cmds:
      - golangci-lint run

  test:
    desc: Run unit tests with the race detector
    cmds:
      - go test -race -covermode=atomic -coverprofile=coverage.out {{.CLI_ARGS | default "./..."}}

  test:examples:
    desc: Run the example module's tests
    dir: examples
    cmds:
      - go mod tidy
      - go test -race -count=1 ./...

  secrets:
    desc: Scan the repository tree for secrets and private keys
    cmds:
      - gitleaks dir --no-banner --redact --verbose .

  vuln:
    desc: Check dependencies for known vulnerabilities
    cmds:
      - govulncheck ./...

  build:
    desc: Build every package
    cmds:
      - go build ./...

  verify:
    desc: Run all local and CI checks
    cmds:
      - task: deps:check
      - task: lint
      - task: secrets
      - task: test
      - task: build

  default:
    cmds:
      - task --list
```

`test:examples` is deliberately absent from `verify` until Task 13 creates the example module; Task 13 adds it.

- [ ] **Step 4: Write the placeholder package**

Create `doc.go`:

```go
// Package relay proxies a stream protocol between clients and upstream
// servers without knowing what the protocol is.
//
// A consumer supplies message boundaries through a Framer and gets a working
// proxy. Supplying a Codec additionally makes decoded packets visible to hooks
// and sinks, and supplying a Prober replaces the default TCP-dial health check
// with one that speaks the protocol. Nothing in this module imports anything
// outside the standard library, which is what makes that claim checkable
// rather than aspirational.
package relay
```

- [ ] **Step 5: Write `.gitignore` and `README.md`**

```bash
printf 'coverage.out\n.devbox/\n.task/\n' > .gitignore
```

`README.md` needs the module path, a one-paragraph description matching `doc.go`, and a note that `examples/` is a separate module. Keep it short; Task 16 expands it once the API is real.

- [ ] **Step 6: Move the spec in**

```bash
mkdir -p docs
cp ../minecraft-protocol/docs/superpowers/specs/2026-08-16-relay-proxy-framework-design.md docs/
```

- [ ] **Step 7: Write the CI workflow**

Create `.github/workflows/ci.yml` running, on push and pull request: devbox install, then `devbox run -- task verify`. Model it on `../minecraft-protocol/.github/workflows/` if one exists; otherwise use `jetify-com/devbox-install-action@v0.13.0` followed by the single verify step.

- [ ] **Step 8: Verify the gate works**

Run: `devbox run -- task verify`
Expected: PASS.

Then prove the gate has teeth:

```bash
printf '\nrequire example.com/x v1.0.0\n' >> go.mod
devbox run -- task deps:check   # expected: FAIL
git checkout go.mod 2>/dev/null || sed -i '/example.com\/x/d;/^require /d' go.mod
devbox run -- task deps:check   # expected: PASS
```

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "chore: bootstrap the relay module and its toolchain"
```

---

### Task 2: Message, actions, and errors

**Files:**
- Create: `message.go`, `errors.go`, `message_test.go`

**Interfaces:**
- Produces: `Direction` (`ToServer`, `ToClient`), `Action` (`Forward`, `Drop`, `Replace`), `Descriptor{ID int32; Name string}`, `Message` with `Raw []byte`, `Decoded any`, `Desc Descriptor`, methods `SetRaw([]byte)`, `SetDecoded(any)`, `DecodedChanged() bool`, `RawChanged() bool`. Sentinels `ErrInvalidConfig`, `ErrNoHealthyUpstream`, `ErrSessionClosed`, `ErrMessageTooLarge`, `ErrHook`.

- [ ] **Step 1: Write the failing test**

Create `message_test.go`:

```go
package relay

import (
	"bytes"
	"testing"
)

func TestDirectionString(t *testing.T) {
	if got := ToServer.String(); got != "to_server" {
		t.Fatalf("ToServer.String() = %q, want to_server", got)
	}
	if got := ToClient.String(); got != "to_client" {
		t.Fatalf("ToClient.String() = %q, want to_client", got)
	}
}

func TestMessageTracksMutation(t *testing.T) {
	m := &Message{Dir: ToServer, Raw: []byte("abc")}

	if m.RawChanged() || m.DecodedChanged() {
		t.Fatal("a fresh message reports itself modified")
	}

	m.SetRaw([]byte("defg"))
	if !m.RawChanged() {
		t.Fatal("SetRaw did not mark the message modified")
	}
	if !bytes.Equal(m.Raw, []byte("defg")) {
		t.Fatalf("Raw = %q, want defg", m.Raw)
	}
	if m.DecodedChanged() {
		t.Fatal("SetRaw marked the decoded value modified")
	}

	m.SetDecoded("value")
	if !m.DecodedChanged() {
		t.Fatal("SetDecoded did not mark the decoded value modified")
	}
	if m.Decoded != "value" {
		t.Fatalf("Decoded = %v, want value", m.Decoded)
	}
}

func TestMessageResetClearsEverything(t *testing.T) {
	m := &Message{Dir: ToClient, Raw: []byte("abc"), Desc: Descriptor{ID: 7, Name: "n"}}
	m.SetDecoded("v")

	m.reset()

	if m.Raw != nil || m.Decoded != nil || m.Desc != (Descriptor{}) {
		t.Fatalf("reset left state behind: %+v", m)
	}
	if m.RawChanged() || m.DecodedChanged() {
		t.Fatal("reset left the modification flags set")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `devbox run -- task test -- ./...`
Expected: FAIL — undefined `ToServer`, `Message`, and the rest.

- [ ] **Step 3: Write the implementation**

Create `message.go`:

```go
package relay

// Direction names which peer a message is travelling towards.
type Direction uint8

const (
	// ToServer is a message read from the client, bound for the upstream.
	ToServer Direction = iota
	// ToClient is a message read from the upstream, bound for the client.
	ToClient
)

// String implements fmt.Stringer. The values are written for logs and for a
// sink's storage column, so they are stable identifiers rather than prose.
func (d Direction) String() string {
	if d == ToClient {
		return "to_client"
	}

	return "to_server"
}

// Opposite returns the direction a reply travels.
func (d Direction) Opposite() Direction {
	if d == ToClient {
		return ToServer
	}

	return ToClient
}

// Action is what a hook wants done with the message it was handed.
type Action uint8

const (
	// Forward sends the message on unchanged.
	Forward Action = iota
	// Drop discards the message and stops the chain. Later hooks do not run,
	// because a message that will never be sent has nothing left to decide.
	Drop
	// Replace sends the message as the hook left it. The chain continues, so a
	// later hook observes the edit rather than the original.
	Replace
)

// Descriptor identifies a decoded packet for logging and dispatch. It is zero
// when no codec ran.
type Descriptor struct {
	ID   int32
	Name string
}

// Message is one framed message in flight.
//
// Raw is drawn from a pool and is valid only for the duration of a hook call.
// A hook that needs the bytes afterwards must copy them. This is the same
// ownership rule the middleware layer of our protocol library documents, and
// it exists for the same reason: a per-message allocation on a proxy holding
// thousands of sessions is not free.
type Message struct {
	Dir     Direction
	Raw     []byte
	Decoded any
	Desc    Descriptor

	rawChanged     bool
	decodedChanged bool
}

// SetRaw replaces the wire bytes and records that re-sending must use them.
func (m *Message) SetRaw(raw []byte) {
	m.Raw = raw
	m.rawChanged = true
}

// SetDecoded replaces the decoded value and records that the message must be
// re-encoded before it is sent. Assigning to Message.Decoded directly does not
// count: the relay would have no way to know the bytes went stale.
func (m *Message) SetDecoded(value any) {
	m.Decoded = value
	m.decodedChanged = true
}

// RawChanged reports whether a hook replaced the wire bytes.
func (m *Message) RawChanged() bool { return m.rawChanged }

// DecodedChanged reports whether a hook replaced the decoded value, which is
// what obliges the relay to re-encode.
func (m *Message) DecodedChanged() bool { return m.decodedChanged }

// reset returns the message to its zero state for reuse by the pool.
func (m *Message) reset() {
	m.Raw = nil
	m.Decoded = nil
	m.Desc = Descriptor{}
	m.rawChanged = false
	m.decodedChanged = false
}
```

Create `errors.go`:

```go
package relay

import "errors"

var (
	// ErrInvalidConfig reports a configuration that cannot produce a running
	// proxy. Every such fault is reported from New, rather than as a nil
	// dereference on the first connection.
	ErrInvalidConfig = errors.New("relay: invalid config")
	// ErrNoHealthyUpstream reports that every candidate for a port failed its
	// probe or its dial.
	ErrNoHealthyUpstream = errors.New("relay: no healthy upstream")
	// ErrSessionClosed reports work attempted on a session that has shut down,
	// which is what an injection racing a disconnect looks like.
	ErrSessionClosed = errors.New("relay: session closed")
	// ErrMessageTooLarge reports a framer returning more bytes than
	// Config.MaxMessageSize allows.
	ErrMessageTooLarge = errors.New("relay: message too large")
	// ErrHook wraps whatever a hook returned or panicked with, so a caller can
	// tell a hook failure from a transport failure without a type switch.
	ErrHook = errors.New("relay: hook failed")
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `devbox run -- task test -- ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add message.go errors.go message_test.go
git commit -m "feat: add the message, action, and error vocabulary"
```

---

### Task 3: The consumer interfaces and the framer conformance harness

**Files:**
- Create: `framer.go`, `hook.go`, `sink.go`, `relaytest/framer.go`, `testframer_test.go`, `relaytest/framer_test.go`

**Interfaces:**
- Consumes: `Direction`, `Descriptor`, `Action`, `Message` from Task 2.
- Produces: `Reader`, `Framer`, `Codec`, `Prober`, `DialProber`, `Hook`, `HookFunc`, `PreFrame`, `PreFrameResult`, `Sink`, `SessionInfo`, `MessageRecord`, `relaytest.FramerContract`, and the unexported `lineFramer` every later core test relays over.

`Framer.ReadMessage` takes a `Reader` interface rather than a `*bufio.Reader`. Read the "Mid-stream transforms" section of the spec for why before writing this task: the conduit in Task 4 has to be the outermost buffer, so it cannot hand out a `*bufio.Reader`, and retrofitting that signature after the harness and every test double were written against a concrete type is a lot of churn to avoid for free here.

- [ ] **Step 1: Write the failing test**

Create `testframer_test.go` — the newline framer the whole core test suite uses. It lives in a `_test.go` file so the zero-dependency module never ships it:

```go
package relay

import (
	"bufio"
	"bytes"
	"io"
	"testing"
)

// lineFramer frames on a newline. It is the core's stand-in for a real
// protocol: enough structure to have boundaries, no dependency to acquire.
type lineFramer struct{}

func (lineFramer) ReadMessage(r Reader) ([]byte, error) {
	var line []byte
	for {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == '\n' {
			return line, nil
		}

		line = append(line, b)
	}
}

func (lineFramer) WriteMessage(w io.Writer, raw []byte) error {
	if _, err := w.Write(append(append([]byte(nil), raw...), '\n')); err != nil {
		return err
	}

	return nil
}

func TestLineFramerRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	f := lineFramer{}

	if err := f.WriteMessage(&buf, []byte("hello")); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	got, err := f.ReadMessage(bufio.NewReader(&buf))
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("ReadMessage = %q, want hello", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `devbox run -- task test -- ./...`
Expected: PASS for this file alone (it defines everything it uses). This step exists to confirm the harness compiles before the interfaces below constrain it.

- [ ] **Step 3: Write the interfaces**

Create `framer.go`:

```go
package relay

import (
	"context"
	"io"
	"net"
	"time"
)

// Reader is the read side a Framer sees.
//
// It is an interface rather than *bufio.Reader because in a running session the
// read side is a *Conduit, which has to be the outermost buffer so that a
// mid-stream transform swap can tell whether any bytes are still unread. A
// *bufio.Reader satisfies it too, which is what lets relaytest exercise a
// Framer over a plain buffer with no session in sight.
//
// Peek is deliberately not part of it: peeking past a transform means
// transforming bytes without consuming them, and a stream cipher cannot be
// asked to do that. A PreFrame hook, which genuinely needs Peek, runs before
// any swap can have happened and is handed the raw buffered reader instead.
type Reader interface {
	io.Reader
	io.ByteReader
}

// Framer turns a byte stream into messages and back. It is the one interface a
// consumer must implement, and the only thing the core needs in order to
// proxy a protocol it knows nothing else about.
//
// ReadMessage must return exactly one complete message, or an error. io.EOF
// means the peer closed cleanly. The returned slice is handed to hooks and may
// be retained by the relay until the message is written, so an implementation
// must not reuse the buffer it returns.
//
// WriteMessage must write every byte or report an error. A short write that
// reports success desynchronises the stream for good.
type Framer interface {
	ReadMessage(Reader) ([]byte, error)
	WriteMessage(io.Writer, []byte) error
}

// Codec is the optional second half: it makes decoded packets visible to hooks
// and sinks.
//
// Decode returns the descriptor alongside the value because the decoder
// already knows the identity, and recovering it through a second dispatch
// would be waste on the hot path.
//
// A Decode error does not end the session. The message is forwarded as opaque
// bytes with a zero Descriptor, because a proxy that refuses to relay what it
// cannot parse is less useful than the connection it replaced.
type Codec interface {
	Decode(Direction, []byte) (any, Descriptor, error)
	Encode(any) ([]byte, error)
}

// Prober reports whether an upstream is usable. A nil error means healthy.
//
// The default speaks no protocol, so it can only tell that something holds the
// port open. A consumer that implements this properly gets health that means
// the server answered.
type Prober interface {
	Probe(ctx context.Context, addr string) error
}

// DialProber is the default Prober: a TCP dial that is closed immediately.
type DialProber struct {
	Timeout time.Duration
}

// Probe implements Prober.
func (p DialProber) Probe(ctx context.Context, addr string) error {
	timeout := p.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}

	return conn.Close()
}
```

Create `hook.go`:

```go
package relay

import (
	"bufio"
	"context"
)

// Hook observes and may alter one message.
//
// The Message it receives is valid only for the duration of the call. A hook
// that wants the bytes afterwards must copy them.
type Hook interface {
	OnMessage(context.Context, *Session, *Message) (Action, error)
}

// HookFunc adapts a function to Hook.
type HookFunc func(context.Context, *Session, *Message) (Action, error)

// OnMessage implements Hook.
func (f HookFunc) OnMessage(ctx context.Context, s *Session, m *Message) (Action, error) {
	return f(ctx, s, m)
}

// PreFrameResult is what a pre-frame hook decided.
type PreFrameResult uint8

const (
	// Continue proceeds to normal framed relaying.
	Continue PreFrameResult = iota
	// Handled means the hook consumed the connection itself. The session ends
	// without dialling an upstream.
	Handled
)

// PreFrame inspects the opening bytes of a client connection before any
// framing happens.
//
// It exists so a session can recognise a protocol it should answer directly
// rather than relay — a legacy ping, a health check, a protocol probe. The
// reader it receives is the same one the read pump will use, so bytes the hook
// consumes are gone from the stream and bytes it only peeks are not.
type PreFrame interface {
	OnConnect(context.Context, *Session, *bufio.Reader) (PreFrameResult, error)
}
```

Create `sink.go`:

```go
package relay

import (
	"context"
	"time"
)

// SessionInfo describes a session at the moment it opened.
type SessionInfo struct {
	ClientAddr   string
	UpstreamAddr string
	Port         int
	OpenedAt     time.Time
}

// MessageRecord is one message as a sink sees it.
//
// Raw is borrowed for the duration of the call, like everywhere else a message
// crosses an interface. A sink that stores it must copy.
type MessageRecord struct {
	Dir     Direction
	Desc    Descriptor
	Raw     []byte
	Decoded any
	At      time.Time
}

// Sink records what crossed the wire.
//
// Only OpenSession returns an error, and no method may block: batching and
// asynchrony belong to the implementation, which can size its own queue for
// its own storage. A core that owned that goroutine could not tune it for
// anyone. A sink that blocks stalls a session's read pump and, through
// backpressure, its peer.
type Sink interface {
	OpenSession(context.Context, SessionInfo) (int64, error)
	Message(context.Context, int64, MessageRecord)
	RawChunk(context.Context, int64, Direction, []byte)
	CloseSession(context.Context, int64)
}

// nopSink is what a proxy configured without a sink uses, so the session path
// never branches on nil.
type nopSink struct{}

func (nopSink) OpenSession(context.Context, SessionInfo) (int64, error) { return 0, nil }
func (nopSink) Message(context.Context, int64, MessageRecord)           {}
func (nopSink) RawChunk(context.Context, int64, Direction, []byte)      {}
func (nopSink) CloseSession(context.Context, int64)                     {}
```

- [ ] **Step 4: Write the conformance harness test first**

Create `relaytest/framer_test.go`:

```go
package relaytest_test

import (
	"io"
	"testing"

	"github.com/go-theft-craft/relay"
	"github.com/go-theft-craft/relay/relaytest"
)

type lineFramer struct{}

func (lineFramer) ReadMessage(r relay.Reader) ([]byte, error) {
	var line []byte
	for {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == '\n' {
			return line, nil
		}

		line = append(line, b)
	}
}

func (lineFramer) WriteMessage(w io.Writer, raw []byte) error {
	_, err := w.Write(append(append([]byte(nil), raw...), '\n'))

	return err
}

func TestFramerContractAcceptsACorrectFramer(t *testing.T) {
	relaytest.FramerContract(t, func() relay.Framer { return lineFramer{} }, [][]byte{
		[]byte("a"),
		[]byte("hello world"),
	})
}

// shortWriteFramer reports success after writing one byte, which is the
// failure the harness exists to catch.
type shortWriteFramer struct{ lineFramer }

func (shortWriteFramer) WriteMessage(w io.Writer, raw []byte) error {
	_, _ = w.Write(raw[:1])

	return nil
}

func TestFramerContractRejectsAShortWrite(t *testing.T) {
	fake := &testing.T{}
	relaytest.FramerContract(fake, func() relay.Framer { return shortWriteFramer{} }, [][]byte{
		[]byte("hello"),
	})
	if !fake.Failed() {
		t.Fatal("FramerContract passed a framer that writes one byte and claims success")
	}
}
```

- [ ] **Step 5: Run test to verify it fails**

Run: `devbox run -- task test -- ./relaytest`
Expected: FAIL — no such package `relaytest`.

- [ ] **Step 6: Write the harness**

Create `relaytest/framer.go`:

```go
// Package relaytest checks a consumer's Framer against the contract the relay
// depends on.
//
// A Framer is the easiest part of the framework to get subtly wrong: partial
// reads, short writes, and a buffer reused after it was handed over all
// produce corruption a long way from their cause. Running this harness turns
// those into a test failure in the consumer's own suite.
package relaytest

import (
	"bufio"
	"bytes"
	"io"
	"testing"

	"github.com/go-theft-craft/relay"
)

// FramerContract exercises newFramer against every message in messages.
//
// newFramer is called per case rather than once, so a stateful framer is
// tested from a known start each time.
func FramerContract(t *testing.T, newFramer func() relay.Framer, messages [][]byte) {
	t.Helper()

	for _, want := range messages {
		roundTrip(t, newFramer(), want)
		oneByteAtATime(t, newFramer(), want)
		truncated(t, newFramer(), want)
		bufferIsOwned(t, newFramer(), want)
	}

	backToBack(t, newFramer(), messages)
}

func roundTrip(t *testing.T, f relay.Framer, want []byte) {
	t.Helper()

	var buf bytes.Buffer
	if err := f.WriteMessage(&buf, want); err != nil {
		t.Fatalf("WriteMessage(%q): %v", want, err)
	}

	got, err := f.ReadMessage(bufio.NewReader(&buf))
	if err != nil {
		t.Fatalf("ReadMessage after writing %q: %v", want, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("round trip gave %q, want %q", got, want)
	}
	if buf.Len() != 0 {
		t.Fatalf("ReadMessage left %d bytes unconsumed after %q", buf.Len(), want)
	}
}

// oneByteAtATime proves the framer reads to a boundary rather than to
// whatever one Read happened to return.
func oneByteAtATime(t *testing.T, f relay.Framer, want []byte) {
	t.Helper()

	var buf bytes.Buffer
	if err := f.WriteMessage(&buf, want); err != nil {
		t.Fatalf("WriteMessage(%q): %v", want, err)
	}

	got, err := f.ReadMessage(bufio.NewReader(iotest{data: buf.Bytes()}.reader()))
	if err != nil {
		t.Fatalf("ReadMessage over a one-byte reader for %q: %v", want, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("one-byte read gave %q, want %q", got, want)
	}
}

// truncated proves an incomplete frame is an error rather than a short
// message. Silently returning what arrived is the corruption this catches.
func truncated(t *testing.T, f relay.Framer, want []byte) {
	t.Helper()

	var buf bytes.Buffer
	if err := f.WriteMessage(&buf, want); err != nil {
		t.Fatalf("WriteMessage(%q): %v", want, err)
	}

	encoded := buf.Bytes()
	if len(encoded) < 2 {
		return
	}

	got, err := f.ReadMessage(bufio.NewReader(bytes.NewReader(encoded[:len(encoded)-1])))
	if err == nil {
		t.Fatalf("ReadMessage returned %q from a truncated frame, want an error", got)
	}
}

// bufferIsOwned proves the framer does not hand back a buffer it will reuse.
func bufferIsOwned(t *testing.T, f relay.Framer, want []byte) {
	t.Helper()

	var buf bytes.Buffer
	for range 2 {
		if err := f.WriteMessage(&buf, want); err != nil {
			t.Fatalf("WriteMessage(%q): %v", want, err)
		}
	}

	br := bufio.NewReader(&buf)
	first, err := f.ReadMessage(br)
	if err != nil {
		t.Fatalf("first ReadMessage(%q): %v", want, err)
	}
	held := append([]byte(nil), first...)

	if _, err := f.ReadMessage(br); err != nil {
		t.Fatalf("second ReadMessage(%q): %v", want, err)
	}
	if !bytes.Equal(first, held) {
		t.Fatalf("the second read overwrote the first message: %q became %q", held, first)
	}
}

// backToBack proves boundaries hold when messages arrive in one buffer.
func backToBack(t *testing.T, f relay.Framer, messages [][]byte) {
	t.Helper()

	var buf bytes.Buffer
	for _, want := range messages {
		if err := f.WriteMessage(&buf, want); err != nil {
			t.Fatalf("WriteMessage(%q): %v", want, err)
		}
	}

	br := bufio.NewReader(&buf)
	for _, want := range messages {
		got, err := f.ReadMessage(br)
		if err != nil {
			t.Fatalf("ReadMessage for %q in a back-to-back stream: %v", want, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("back-to-back read gave %q, want %q", got, want)
		}
	}

	if _, err := f.ReadMessage(br); err != io.EOF {
		t.Fatalf("ReadMessage at end of stream returned %v, want io.EOF", err)
	}
}

// iotest yields one byte per Read, which is what a real socket does under a
// small MTU and what a framer that trusts one Read gets wrong.
type iotest struct{ data []byte }

func (i iotest) reader() io.Reader { return &oneByteReader{data: i.data} }

type oneByteReader struct {
	data []byte
	at   int
}

func (r *oneByteReader) Read(p []byte) (int, error) {
	if r.at >= len(r.data) {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}

	p[0] = r.data[r.at]
	r.at++

	return 1, nil
}
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `devbox run -- task test -- ./...`
Expected: PASS. Note that `TestFramerContractRejectsAShortWrite` relies on `FramerContract` reporting through `t.Fatalf` on the passed-in `*testing.T`; because `Fatalf` on a synthetic `*testing.T` does not unwind the calling goroutine the way it does in a real test, the harness must reach the write check before any `Fatalf`. It does — `roundTrip` writes first.

- [ ] **Step 8: Commit**

```bash
git add framer.go hook.go sink.go testframer_test.go relaytest/
git commit -m "feat: add the consumer interfaces and a framer conformance harness"
```

---

### Task 4: The conduit and mid-stream transform swaps

**Files:**
- Create: `conduit.go`, `conduit_test.go`

**Interfaces:**
- Consumes: `Reader`, `Framer` from Task 3.
- Produces: `Transform`, `Conduit`, `Conduit.Swap`, and `ErrSwapPending`. Task 6's read pumps are written against `Conduit`, which is why this task precedes them.

Read the "Mid-stream transforms" section of the spec before starting, and read `conduit.go` in `minecraft-protocol` — this is the same problem, already solved once, and the ordering it found is the whole design. Do not reinvent it.

The one rule everything follows from: **buffer raw bytes, and transform them as they are handed out rather than as they are buffered.** Under that ordering a swap never has to reach into the buffer, rebuild a reader chain, or interrupt a parked pump. It is a field assignment under a mutex, safe from any goroutine.

- [ ] **Step 1: Write the failing test**

Create `conduit_test.go`:

```go
package relay

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// flip inverts every byte in place. It is its own inverse, which makes it a
// stand-in for a stream cipher that a test can assert against without a key
// exchange.
func flip(p []byte) {
	for i := range p {
		p[i] = ^p[i]
	}
}

func flipped(b []byte) []byte {
	out := append([]byte(nil), b...)
	flip(out)

	return out
}

func flipTransform() Transform {
	return Transform{
		Read: flip,
		Write: func(p []byte) []byte { return flipped(p) },
	}
}

// TestConduitSwapWhileParked is the case the hand-out ordering exists for: the
// pump is blocked inside a socket read when the swap lands, and the bytes that
// arrive afterwards must come out transformed.
func TestConduitSwapWhileParked(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	c := NewConduit(server, 4096)
	f := lineFramer{}

	type result struct {
		msg []byte
		err error
	}
	done := make(chan result, 1)

	go func() {
		msg, err := f.ReadMessage(c)
		done <- result{msg: msg, err: err}
	}()

	// Let the reader park inside the pipe read with nothing buffered.
	time.Sleep(50 * time.Millisecond)

	if err := c.Swap(flipTransform()); err != nil {
		t.Fatalf("Swap while parked: %v", err)
	}

	if _, err := client.Write(flipped([]byte("late\n"))); err != nil {
		t.Fatalf("Write: %v", err)
	}

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("ReadMessage after a swap: %v", got.err)
		}
		if string(got.msg) != "late" {
			t.Fatalf("message = %q, want late", got.msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ReadMessage never returned")
	}
}

// TestConduitSwapRefusesBufferedBytes covers the one refusal. Bytes already
// buffered arrived before the switch, so transforming them on the way out would
// corrupt the next message with nothing to point at afterwards.
func TestConduitSwapRefusesBufferedBytes(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	c := NewConduit(server, 4096)
	f := lineFramer{}

	go func() { _, _ = client.Write([]byte("trigger\nextra\n")) }()

	first, err := f.ReadMessage(c)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if string(first) != "trigger" {
		t.Fatalf("first message = %q, want trigger", first)
	}
	if c.Buffered() == 0 {
		t.Fatal("the test needs bytes left buffered; there were none")
	}

	if err := c.Swap(flipTransform()); !errors.Is(err, ErrSwapPending) {
		t.Fatalf("Swap with %d bytes buffered returned %v, want ErrSwapPending", c.Buffered(), err)
	}
}

func TestConduitReadTransformsOnHandout(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	c := NewConduit(server, 4096)
	if err := c.Swap(flipTransform()); err != nil {
		t.Fatalf("Swap: %v", err)
	}

	go func() { _, _ = client.Write(flipped([]byte("hello\n"))) }()

	got, err := lineFramer{}.ReadMessage(c)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("message = %q, want hello", got)
	}
}

func TestConduitWriteTransform(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	c := NewConduit(server, 4096)
	if err := c.Swap(flipTransform()); err != nil {
		t.Fatalf("Swap: %v", err)
	}

	go func() { _ = lineFramer{}.WriteMessage(c, []byte("out")) }()

	got := make([]byte, 4)
	if _, err := io.ReadFull(client, got); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	if !bytes.Equal(got, flipped([]byte("out\n"))) {
		t.Fatalf("wrote %q, want the flipped form of out\\n", got)
	}
}

// TestConduitWriteDoesNotMutateCaller matters because the message the relay is
// writing is the same slice a sink or a hook may still be holding.
func TestConduitWriteDoesNotMutateCaller(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	c := NewConduit(server, 4096)
	if err := c.Swap(flipTransform()); err != nil {
		t.Fatalf("Swap: %v", err)
	}

	payload := []byte("keepme")
	held := append([]byte(nil), payload...)

	go func() { _, _ = io.CopyN(io.Discard, client, int64(len(payload))) }()

	if _, err := c.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !bytes.Equal(payload, held) {
		t.Fatalf("Write mutated the caller's buffer: %q became %q", held, payload)
	}
}

// TestConduitWriteReportsCallerBytes pins the io.Writer contract for a
// transform that changes length. Returning the transformed count would make
// io.Copy and bufio believe a different number of bytes moved than the caller
// handed over, and the bug would surface far from here.
func TestConduitWriteReportsCallerBytes(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	c := NewConduit(server, 4096)

	// Doubling stands in for compression: the only property that matters is
	// that the transformed length differs from the caller's.
	err := c.Swap(Transform{Write: func(p []byte) []byte {
		return append(append([]byte(nil), p...), p...)
	}})
	if err != nil {
		t.Fatalf("Swap: %v", err)
	}

	payload := []byte("grow")
	go func() { _, _ = io.CopyN(io.Discard, client, int64(2*len(payload))) }()

	n, werr := c.Write(payload)
	if werr != nil {
		t.Fatalf("Write: %v", werr)
	}
	if n != len(payload) {
		t.Fatalf("Write returned %d, want %d — the count must be in the caller's bytes", n, len(payload))
	}
}

func TestConduitComposesSwaps(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	c := NewConduit(server, 4096)
	if err := c.Swap(flipTransform()); err != nil {
		t.Fatalf("first Swap: %v", err)
	}
	if err := c.Swap(flipTransform()); err != nil {
		t.Fatalf("second Swap: %v", err)
	}

	// Two flips compose to the identity, which is how the test knows the second
	// swap layered over the first rather than replacing it.
	go func() { _, _ = client.Write([]byte("plain\n")) }()

	got, err := lineFramer{}.ReadMessage(c)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if string(got) != "plain" {
		t.Fatalf("message = %q, want plain", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `devbox run -- task test -- ./...`
Expected: FAIL — undefined `NewConduit`, `Transform`, `ErrSwapPending`.

- [ ] **Step 3: Write the implementation**

Create `conduit.go`:

```go
package relay

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sync"
)

// Transform is a change to how one direction's bytes are encoded, from some
// message boundary onwards. A nil half leaves that side of the stream alone.
//
// It exists because framing is not constant for the life of a connection: real
// protocols negotiate a compression threshold or a cipher partway through, and
// every byte after the agreed one is encoded differently.
//
// Read transforms in place, because the conduit owns the buffer it is handed
// and an allocation per read on a proxy holding thousands of sessions is not
// free. Write returns a new slice, because the caller owns the message it
// passed and a hook or a sink may still be holding a view of it.
type Transform struct {
	Read  func([]byte)
	Write func([]byte) []byte
}

// Conduit is one direction's byte layer: the socket, its raw buffer, and the
// transform currently applied to bytes crossing it.
//
// It buffers raw bytes and transforms them as it hands them out, not as it
// buffers them. That ordering is the whole design. Because the buffer holds
// untransformed bytes, a swap never has to reach into it or rebuild anything
// above it; because the lock is never held around a socket read, a swap can
// land while the read pump is parked, which is exactly when a cipher
// negotiated in the other direction needs to be installed here.
//
// A Conduit is safe for concurrent use by one reader, one writer, and any
// number of goroutines calling Swap.
type Conduit struct {
	conn     net.Conn
	buffered *bufio.Reader

	mu    sync.Mutex
	read  func([]byte)
	write func([]byte) []byte
	// pending is how many raw bytes the buffer still holds, recorded by the
	// reader under the mutex.
	//
	// Swap cannot ask the bufio.Reader directly: the pump is normally parked
	// inside a socket read when a swap arrives, and reading bufio state
	// concurrently with that is a data race. The reader publishes the count
	// instead, which is exact for the same reason the swap is safe at all — a
	// parked read has buffered nothing, so the last recorded count still holds.
	pending int
}

// NewConduit wraps a connection. bufSize is the raw read buffer; at ten
// thousand sessions the default is worth roughly 80 MiB, which is what makes it
// a knob rather than a constant.
func NewConduit(conn net.Conn, bufSize int) *Conduit {
	if bufSize <= 0 {
		bufSize = 4096
	}

	return &Conduit{conn: conn, buffered: bufio.NewReaderSize(conn, bufSize)}
}

// PreFrameReader returns the raw buffered reader a PreFrame hook inspects.
//
// The hook runs before any message and therefore before any swap, so reading
// the buffer directly is identical to reading through the conduit. This is the
// one place Peek is available, and the reason Reader does not offer it.
func (c *Conduit) PreFrameReader() *bufio.Reader { return c.buffered }

// Read implements io.Reader, applying the active read transform to the bytes as
// they are handed out.
func (c *Conduit) Read(p []byte) (int, error) {
	n, err := c.buffered.Read(p)

	// The lock is taken after the read, never around it, so a socket read that
	// blocks forever cannot stop another goroutine from swapping.
	c.mu.Lock()
	if n > 0 && c.read != nil {
		c.read(p[:n])
	}
	c.pending = c.buffered.Buffered()
	c.mu.Unlock()

	return n, err
}

// ReadByte implements io.ByteReader, which together with Read satisfies Reader.
func (c *Conduit) ReadByte() (byte, error) {
	var one [1]byte
	if _, err := io.ReadFull(c, one[:]); err != nil {
		return 0, err
	}

	return one[0], nil
}

// Write implements io.Writer. It never retains p and never mutates it.
//
// The count returned is always in the caller's bytes, never the transformed
// ones, and the write is all-or-nothing. A transform may change length --
// compression is the obvious case -- so there is no honest way to express a
// partial write in terms of p: half a compressed block is not half a message,
// and no caller could resume from it. Reporting the transformed count instead
// would be worse than useless, because io.Copy and bufio both believe it.
func (c *Conduit) Write(p []byte) (int, error) {
	c.mu.Lock()
	active := c.write
	c.mu.Unlock()

	out := p
	if active != nil {
		out = active(p)
	}

	if _, err := c.conn.Write(out); err != nil {
		return 0, err
	}

	return len(p), nil
}

// Buffered reports how many raw bytes are waiting to be handed out.
func (c *Conduit) Buffered() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.pending
}

// Swap installs a transform over whatever is already active, so a session that
// enables compression and later encryption ends up with both, in that order.
//
// It refuses when the read buffer still holds unread bytes: those arrived
// before the switch and belong to the old encoding, and transforming them on
// the way out would corrupt the very next message with nothing to point at
// afterwards. Failing here names the cause at the cause.
//
// In practice the buffer is empty exactly when it should be. Every protocol
// that renegotiates mid-stream requires the peer to stop sending across the
// boundary, because both endpoints have the same problem and solve it the same
// way. A non-empty buffer means the peer broke that rule or the caller swapped
// at the wrong message, and both deserve an error.
func (c *Conduit) Swap(t Transform) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.pending > 0 {
		return fmt.Errorf("%w: %d unread bytes", ErrSwapPending, c.pending)
	}

	if t.Read != nil {
		prior := c.read
		next := t.Read
		if prior == nil {
			c.read = next
		} else {
			c.read = func(p []byte) { prior(p); next(p) }
		}
	}

	if t.Write != nil {
		prior := c.write
		next := t.Write
		if prior == nil {
			c.write = next
		} else {
			c.write = func(p []byte) []byte { return next(prior(p)) }
		}
	}

	return nil
}
```

Add to `errors.go`:

```go
// ErrSwapPending reports a mid-stream transform swap attempted while bytes
// from before the boundary were still unread. See Conduit.Swap.
ErrSwapPending = errors.New("relay: swap with bytes still buffered")
```

Note the composition order. Read transforms compose outermost-last — bytes come off the wire and pass through the oldest transform first — while write transforms compose in the mirror order, so a message passes through the newest first on the way out. Getting this backwards produces a stream that works for one layer and breaks the moment a second is added, which is why the composition test uses two flips.

- [ ] **Step 4: Run test to verify it passes**

Run: `devbox run -- task test -- ./... -count=5`
Expected: PASS under `-race` on every run. The parked-swap test is the one that matters: it exists to catch a lock held around the socket read, and it only fails under `-race` or under load.

- [ ] **Step 5: Commit**

```bash
git add conduit.go conduit_test.go errors.go
git commit -m "feat: add per-direction conduits and mid-stream transform swaps"
```

---

### Task 5: Config, validation, and defaults

**Files:**
- Create: `config.go`, `config_test.go`, `selector.go`, `session.go`

**Interfaces:**
- Consumes: `Framer`, `Codec`, `Prober`, `Sink`, `Hook`, `PreFrame` from Task 3.
- Produces: `Config`, `PortConfig`, `OverflowPolicy`, and `(*Config).validate()`, which every fault in the configuration is reported through.

`Config` names three types that belong to later tasks: `Upstream` and `Selector` (Task 10) and `*Session` (Task 6). Declare them here, empty, in the files that will own them — `Upstream` and the `Selector` interface in `selector.go`, and the `Session` struct with only the fields `Config` needs in `session.go`. Task 6 and Task 10 grow them rather than creating them. Do not put them in `config.go` and move them later; a type that lives in the wrong file for six tasks tends to stay there.

- [ ] **Step 1: Write the failing test**

Create `config_test.go` covering: a config with no ports is `ErrInvalidConfig`; a port with no upstreams is `ErrInvalidConfig`; a nil `Framer` is `ErrInvalidConfig`; a duplicate port number is `ErrInvalidConfig`; and a minimal valid config gets every default filled in — `Prober` becomes a `DialProber`, `Sink` becomes `nopSink`, `Selector` becomes `FirstHealthy()`, and the numeric fields become their documented defaults. Assert with `errors.Is(err, ErrInvalidConfig)` rather than on message text.

Defaults to assert:

| Field | Default | Why |
| --- | --- | --- |
| `ReadBufferSize` | 4096 | 80 MiB across ten thousand sessions, which is what makes it a knob |
| `WriteBufferSize` | 4096 | same |
| `MaxMessageSize` | 2 << 20 | bounds what a hostile length prefix can allocate |
| `MaxSessions` | 0 | unlimited; opting in to a bound is the caller's decision |
| `Overflow` | `OverflowClose` | a client that cannot be served learns so immediately |
| `ProbeTTL` | 10s | a burst against one port costs one probe |
| `ProbeTimeout` | 2s | bounds the accept path |
| `DialTimeout` | 5s | |
| `DrainGrace` | 5s | one in-flight write, not a lingering session |

- [ ] **Step 2: Run test to verify it fails**

Run: `devbox run -- task test -- ./...`
Expected: FAIL — undefined `Config`.

- [ ] **Step 3: Write the implementation**

Create `config.go`. `Config` holds:

```go
type Config struct {
	Ports []PortConfig

	Framer   Framer
	Codec    Codec
	Prober   Prober
	Sink     Sink
	Selector Selector

	Hooks    []Hook
	PreFrame PreFrame

	ReadBufferSize  int
	WriteBufferSize int
	MaxMessageSize  int
	MaxSessions     int
	Overflow        OverflowPolicy

	ProbeTTL     time.Duration
	ProbeTimeout time.Duration
	DialTimeout  time.Duration
	DrainGrace   time.Duration

	OnSessionError func(*Session, error)
	Logger         *slog.Logger

	// now is the clock the probe cache reads. Tests set it; nothing else does,
	// because a probe TTL that can only be exercised by sleeping makes the
	// health tests slow and flaky at the same time.
	now func() time.Time
}

type PortConfig struct {
	Port      int
	Upstreams []Upstream
}

type OverflowPolicy uint8

const (
	// OverflowClose closes a connection that arrives with no session slot free.
	OverflowClose OverflowPolicy = iota
	// OverflowWait holds the connection until a slot frees or the proxy stops.
	OverflowWait
)
```

`validate()` returns wrapped `ErrInvalidConfig` for every fault and fills defaults in place. Every configuration fault must surface from `New`, not as a nil dereference on the first connection — that is the whole reason this method exists rather than scattered nil checks.

`OnSessionError` defaults to a `slog` line at warn level on `Config.Logger`, itself defaulting to `slog.Default()`. `Run` returns only fatal faults, so this is where per-session errors go; with thousands of sessions there is nowhere else they can go.

- [ ] **Step 4: Run test to verify it passes**

Run: `devbox run -- task test -- ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add config.go config_test.go
git commit -m "feat: add proxy configuration, validation, and defaults"
```

---

### Task 6: The session — read pumps, writer locks, hook chain, panic recovery

**Files:**
- Modify: `session.go`
- Create: `session_test.go`

**Interfaces:**
- Consumes: everything from Tasks 2–5.
- Produces: `Session` with `Set`/`Get`/`Snapshot`/`Close`/`Context`, the two read pumps, the per-peer writer lock, hook chain execution, and panic recovery at the session boundary.

This is the largest task in the plan. Build it in the order the steps give, and keep every test on `net.Pipe` — no listener is involved until Task 11.

- [ ] **Step 1: Write the failing tests**

Create `session_test.go` covering, in this order:

1. **Relay in both directions.** A session over two `net.Pipe` pairs relays a client line to the upstream and an upstream line back, byte for byte.
2. **`Drop` stops the chain.** A hook returning `Drop` prevents the message reaching the peer *and* prevents a later hook from running. Assert both — the second is the part that is easy to implement wrongly.
3. **`Replace` continues the chain.** A first hook calls `SetRaw` and returns `Replace`; a second hook observes the edited bytes, not the original; the peer receives the edit.
4. **Re-encode happens once.** With a codec installed, a hook that calls `SetDecoded` causes exactly one `Encode` call after the whole chain, not one per hook. Count the calls.
5. **A decode error does not end the session.** A codec returning an error from `Decode` still forwards the message as opaque bytes with a zero `Descriptor`, and the session stays open.
6. **A hook error ends the session.** The error reaches `OnSessionError` wrapped so `errors.Is(err, ErrHook)` holds, and both connections close.
7. **A hook panic ends only that session.** Two concurrent sessions; a hook panics on one; the other keeps relaying. The reported error satisfies `errors.Is(err, ErrHook)` and its message contains the stack.
8. **`MaxMessageSize` is enforced.** A framer returning more bytes than the limit ends the session with `ErrMessageTooLarge`.
9. **`io.EOF` is a clean close.** `OnSessionError` is not called when a peer closes cleanly.
10. **The sink sees the session.** `OpenSession` runs before any message, `Message` runs per relayed message with the right `Direction`, `CloseSession` runs exactly once.
11. **`Set`/`Get`/`Snapshot`.** Metadata set from a hook is visible in a snapshot, and the snapshot is a copy — mutating it does not affect the session.

Write a `recordingSink` and a `countingCodec` in this file. Both are test doubles; both stay in `_test.go`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `devbox run -- task test -- ./...`
Expected: FAIL — undefined `Session`.

- [ ] **Step 3: Write the session type**

Create `session.go`. The shape:

```go
// Session is one client connection and the upstream it was joined to.
//
// It is passed to every hook and sink call, and is the handle a consumer uses
// to inject messages, attach metadata, swap a mid-stream transform, or end the
// connection.
type Session struct {
	ID       int64
	Client   net.Conn
	Upstream net.Conn
	Info     SessionInfo

	cfg    *Config
	sinkID int64

	toClient *Conduit
	toServer *Conduit

	// One-slot semaphores rather than mutexes, because a write must be able to
	// lose a race with the session context and give up rather than block until
	// a dead peer's write times out.
	clientWrite chan struct{}
	serverWrite chan struct{}

	ctx    context.Context
	cancel context.CancelCauseFunc

	mu   sync.RWMutex
	meta map[string]any

	closeOnce sync.Once
}
```

Key methods and the reasoning each one has to encode:

- `Context() context.Context` — the session context, cancelled on close. Hooks that start work use it.
- `Set(key string, value any)`, `Get(key string) (any, bool)` — consumer metadata under `mu`. The framework never reads the values; the registry only copies them.
- `Snapshot() SessionSnapshot` — a copy of the identity, addresses, open time, and metadata map. Copy the map; handing out the live one turns a listing into a data race.
- `Close()` — idempotent through `closeOnce`, cancels the context with `ErrSessionClosed`.

- [ ] **Step 4: Write the writer lock**

```go
// write sends one framed message to a peer.
//
// The one-slot channel is what makes injection safe: every write to a peer,
// relayed or injected, passes through here, so no two goroutines hold the same
// writer and an injected message can never land inside a relayed one. A
// framework that handed hooks a raw net.Conn could not make that promise.
func (s *Session) write(dir Direction, raw []byte) error {
	lock, c := s.serverWrite, s.toServer
	if dir == ToClient {
		lock, c = s.clientWrite, s.toClient
	}

	select {
	case lock <- struct{}{}:
		defer func() { <-lock }()
	case <-s.ctx.Done():
		return ErrSessionClosed
	}

	return s.cfg.Framer.WriteMessage(c, raw)
}
```

- [ ] **Step 5: Write the read pump**

One function serving both directions, parameterised by direction. It:

1. Calls `cfg.Framer.ReadMessage(c)` on its direction's `*Conduit`, which applies whatever transform is active as it hands the bytes out.
2. Treats `io.EOF` as a clean close and every other read error as a session-ending fault.
3. Rejects a message longer than `cfg.MaxMessageSize` with `ErrMessageTooLarge`.
4. Takes a `*Message` from the pool, fills `Dir` and `Raw`.
5. Decodes if a codec is configured. **A decode error is not fatal** — log through `OnSessionError` at most once per session and carry on with opaque bytes and a zero `Descriptor`. A proxy that refuses to relay what it cannot parse is less useful than the connection it replaced.
6. Runs the hook chain (Step 6).
7. Re-encodes once if `DecodedChanged()` and no hook set raw bytes afterwards; `RawChanged()` wins, because a hook that wrote bytes was more specific than one that wrote a value.
8. Calls `Sink.Message` with the final bytes.
9. Writes to the peer through `s.write`.
10. Returns the message to the pool.

A read pump blocks while writing to a slow peer. That is deliberate: it propagates TCP backpressure to the origin instead of buffering, and a queue between the pumps would cost a goroutine per direction to decouple something nobody asked to decouple.

- [ ] **Step 6: Write the hook chain with panic recovery**

```go
// runHooks walks the chain and returns the action the chain settled on.
//
// Drop stops the walk, because a message that will never be sent has nothing
// left to decide. Replace does not, so a later hook sees the edit rather than
// the original.
func (s *Session) runHooks(ctx context.Context, m *Message) (Action, error) {
	for _, h := range s.cfg.Hooks {
		action, err := s.callHook(ctx, h, m)
		if err != nil {
			return Drop, err
		}
		if action == Drop {
			return Drop, nil
		}
	}

	return Forward, nil
}

// callHook isolates one hook so a panic ends this session and no other.
//
// minecraft-protocol/router deliberately does not recover handler panics: for a
// library driving one connection, burying a bug puts the report far from its
// cause. A proxy holds thousands of sessions, and one malformed message
// reaching a buggy hook must not take the unrelated ones down with it. The
// divergence is deliberate, and the stack is carried on the error rather than
// discarded, so the report still lands near the cause.
func (s *Session) callHook(ctx context.Context, h Hook, m *Message) (action Action, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v\n%s", ErrHook, r, debug.Stack())
			action = Drop
		}
	}()

	action, err = h.OnMessage(ctx, s, m)
	if err != nil {
		return Drop, fmt.Errorf("%w: %w", ErrHook, err)
	}

	return action, nil
}
```

A hook that returns an error ends its session. A hook that meant to rewrite a message and failed has left the stream in a state neither peer agreed to, and forwarding anyway corrupts it quietly.

- [ ] **Step 7: Write the message pool**

A package-level `sync.Pool` of `*Message`. Take on read, `reset()` and put back after the write. `Message.Raw` is borrowed for the duration of a hook call and no longer — the same ownership contract `minecraft-protocol/middleware` documents, so the two repositories read consistently.

- [ ] **Step 8: Write the run and shutdown paths**

`(*Session).run` starts both pumps, waits for the first to fail, cancels the context, gives the peers `Config.DrainGrace` to finish an in-flight write, then closes both connections, calls `Sink.CloseSession` exactly once, and returns. Non-EOF causes go to `Config.OnSessionError`.

- [ ] **Step 9: Add `Session.Swap`**

```go
// Swap installs a mid-stream transform on one direction's conduit.
//
// It is safe from any goroutine, including from a hook running on the other
// direction's pump — which is the common case, since a cipher negotiated in one
// direction has to be installed on both. It returns ErrSwapPending when that
// direction still holds unread bytes from before the boundary; see Conduit.Swap
// for why that is an error rather than something to absorb.
func (s *Session) Swap(dir Direction, t Transform) error {
	c := s.toServer
	if dir == ToClient {
		c = s.toClient
	}

	return c.Swap(t)
}
```

Add a session test for the cross-direction case: a hook on the `ToServer` pump swaps `ToClient` while that pump is parked in a read, and the next message from the upstream arrives transformed. This is the arrangement every real consumer will use, and it is only safe because of the hand-out ordering in Task 4.

- [ ] **Step 10: Run tests to verify they pass**

Run: `devbox run -- task test -- ./...`
Expected: PASS, under `-race`. The writer lock is one of the two places in this module where a race costs data, so a clean race run is the acceptance criterion, not a bonus.

- [ ] **Step 11: Commit**

```bash
git add session.go session_test.go
git commit -m "feat: add session relaying, hook chains, and per-session panic recovery"
```

---

### Task 7: Injection and its ordering guarantee

**Files:**
- Modify: `session.go`
- Create: `inject_test.go`

**Interfaces:**
- Consumes: `Session.write` from Task 6.
- Produces: `Session.Inject(Direction, []byte) error` and `Session.InjectDecoded(Direction, any) error`.

Injection is designed in rather than added later because the guarantee it makes — an injected message never lands inside a relayed one — depends on every write going through the same lock. That is already true after Task 6; this task exposes it and proves it.

- [ ] **Step 1: Write the failing test**

Create `inject_test.go` with a framer built for this test and nothing else:

```go
// splitFramer writes a message in two parts with a scheduling point between
// them. A relay that did not hold a writer lock across the whole message would
// let an injected message land in the gap, and this framer makes that
// interleaving happen every run rather than one run in a thousand.
type splitFramer struct{ lineFramer }

func (f splitFramer) WriteMessage(w io.Writer, raw []byte) error {
	if len(raw) < 2 {
		return f.lineFramer.WriteMessage(w, raw)
	}

	if _, err := w.Write(raw[:len(raw)/2]); err != nil {
		return err
	}

	runtime.Gosched()

	if _, err := w.Write(raw[len(raw)/2:]); err != nil {
		return err
	}

	_, err := w.Write([]byte("\n"))

	return err
}
```

The test: run a session over `net.Pipe`, relay a stream of `AAAA…`-style messages while a second goroutine injects `BBBB…` messages continuously, read every line the peer received, and assert each line is entirely one letter. A line containing both is the interleaving that must never happen. Run it with enough iterations that the scheduling point is exercised — a few hundred is plenty and stays fast.

Also assert that `Inject` on a closed session returns `ErrSessionClosed` rather than blocking forever.

- [ ] **Step 2: Run test to verify it fails**

Run: `devbox run -- task test -- ./...`
Expected: FAIL — undefined `Inject`.

- [ ] **Step 3: Write the implementation**

```go
// Inject sends a message to one peer as though the other had sent it.
//
// It acquires the same writer lock relaying uses, so an injected message never
// interleaves inside a relayed one — the guarantee that makes injection worth
// having, and the one a framework handing out a raw net.Conn cannot make.
//
// Injected messages do not run the hook chain. A hook that wants to see what it
// injected can see it at the point it injected it, and re-entering the chain
// invites a hook that injects on every message to recurse.
func (s *Session) Inject(dir Direction, raw []byte) error {
	select {
	case <-s.ctx.Done():
		return ErrSessionClosed
	default:
	}

	return s.write(dir, raw)
}

// InjectDecoded encodes through the configured Codec and injects the result. It
// returns ErrInvalidConfig when no codec is configured, because the alternative
// is a silent no-op on a call that read like it sent something.
func (s *Session) InjectDecoded(dir Direction, value any) error
```

Record injected messages to the sink as well, with the direction they travelled. A capture that omits what the proxy itself sent is a capture that cannot be replayed.

- [ ] **Step 4: Run test to verify it passes**

Run: `devbox run -- task test -- ./... -count=5`
Expected: PASS on every run. Repeat the run a few times; an ordering test that passes once has proved less than it looks.

- [ ] **Step 5: Commit**

```bash
git add session.go inject_test.go
git commit -m "feat: add message injection with a stated ordering guarantee"
```

---

### Task 8: The session registry

**Files:**
- Create: `registry.go`, `registry_test.go`
- Modify: `session.go`, so a session removes itself from the registry as it finishes

**Interfaces:**
- Consumes: `Session`, `SessionSnapshot` from Task 6.
- Produces: an unexported `registry` and the `Proxy` methods Task 11 exposes over it: `Sessions() []SessionSnapshot`, `SessionCount() int`, `UpstreamCount(addr string) int`.

- [ ] **Step 1: Write the failing test**

Create `registry_test.go`: adding and removing sessions moves the counts; `snapshots()` returns a stable slice that later mutation of the registry does not change; per-upstream counts track add and remove and never go negative; `drain(ctx)` closes every live session and returns when the last one is gone, or when the context expires. Add a test that hammers add, remove, and snapshot from several goroutines — the registry is read by `LeastConn` on the accept path, so it is contended by design.

- [ ] **Step 2: Run test to verify it fails**

Run: `devbox run -- task test -- ./...`
Expected: FAIL — undefined `registry`.

- [ ] **Step 3: Write the implementation**

A `sync.RWMutex` over `map[int64]*Session` plus `map[string]int` for per-upstream counts. The per-upstream map exists so `LeastConn` is a map lookup rather than a walk of every live session on every accept.

`drain(ctx)` calls `Close` on every session and waits on a `sync.WaitGroup` that `run` decrements, honouring `ctx`.

- [ ] **Step 4: Run test to verify it passes**

Run: `devbox run -- task test -- ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add registry.go registry_test.go
git commit -m "feat: track live sessions and per-upstream counts"
```

---

### Task 9: The probe cache

**Files:**
- Create: `health.go`, `health_test.go`

**Interfaces:**
- Consumes: `Prober` from Task 3, `Config.ProbeTTL`, `Config.ProbeTimeout`, and the injectable `Config.now` from Task 5.
- Produces: an unexported `healthCache` with `check(ctx, addrs []string) []string` returning the healthy subset, and `markDown(addr string)` for dial failures.

Health is resolved lazily, when a client connects, rather than on a timer. A startup probe that goes stale is a real bug; a per-connection probe is only viable because of the three properties this task implements.

- [ ] **Step 1: Write the failing test**

Create `health_test.go`. Drive time with a fake clock assigned to `Config.now` — never `time.Sleep`. Cover:

1. **TTL.** Two `check` calls inside the TTL cost one `Probe` call. Advance the clock past the TTL and the next call probes again.
2. **Single flight.** Fifty goroutines calling `check` for the same cold address produce exactly one `Probe` call, and all fifty get the same answer. Use a prober that blocks on a channel so the concurrency is real rather than lucky.
3. **Fan-out.** A `check` over four cold addresses issues four probes concurrently, not serially: a prober that blocks until all four have arrived must not deadlock.
4. **Shared deadline.** With `ProbeTimeout` set and a prober that never returns, `check` returns within the timeout and reports every address unhealthy.
5. **Dial failure writes through.** `markDown(addr)` makes a subsequent `check` report that address unhealthy without probing, until the TTL expires.
6. **Ordering is preserved.** `check` returns the healthy addresses in the order they were given, because `FirstHealthy` depends on it.

- [ ] **Step 2: Run test to verify it fails**

Run: `devbox run -- task test -- ./...`
Expected: FAIL — undefined `healthCache`.

- [ ] **Step 3: Write the implementation**

```go
// healthCache answers "is this upstream usable" without probing on every
// connection.
//
// A result is cached for Config.ProbeTTL, so a burst of clients against one
// port costs one probe rather than one each. Concurrent misses for the same
// address collapse into a single in-flight probe and the rest wait on its
// result. And dial failures write to the same cache as probes, so passive
// signal and active probe share one state instead of disagreeing.
type healthCache struct {
	prober  Prober
	ttl     time.Duration
	timeout time.Duration
	now     func() time.Time

	mu      sync.Mutex
	entries map[string]*healthEntry
}

type healthEntry struct {
	healthy bool
	at      time.Time
	done    chan struct{} // non-nil while a probe is in flight
}
```

`check` builds the list of misses under the lock, installs an in-flight entry for each, releases the lock, probes the misses concurrently under one `context.WithTimeout(ctx, timeout)`, records results, closes the `done` channels, and then assembles the answer in input order. Waiters block on `done` and re-read, rather than probing.

Do not hold `mu` across a `Probe` call. That is the mistake this structure exists to avoid: probing under the lock serialises the fan-out and turns one slow upstream into a stall on every accept.

- [ ] **Step 4: Run test to verify it passes**

Run: `devbox run -- task test -- ./... -count=5`
Expected: PASS under `-race` on every run. The single-flight path is the second of the two places in this module where a race costs data.

- [ ] **Step 5: Commit**

```bash
git add health.go health_test.go
git commit -m "feat: resolve upstream health lazily through a single-flight probe cache"
```

---

### Task 10: Selectors

**Files:**
- Modify: `selector.go`
- Create: `selector_test.go`

**Interfaces:**
- Consumes: `registry` from Task 8.
- Produces: `Upstream`, `Selector`, and `FirstHealthy()`, `RoundRobin()`, `LeastConn()`, `StickyByClientIP()`.

- [ ] **Step 1: Write the failing test**

Create `selector_test.go`:

1. `FirstHealthy` picks the first candidate in the given order, every time.
2. `RoundRobin` cycles, and cycles correctly when the candidate list shrinks between calls — an index that outlives the slice it indexed is the bug here.
3. `LeastConn` picks the upstream with the fewest live sessions and breaks ties towards the earlier candidate.
4. `StickyByClientIP` sends two connections from one IP to the same upstream, sends a different IP somewhere its own hash lands, and stays stable when the port changes but the IP does not — a client reconnecting from a new ephemeral port must land where it did before.
5. Every selector returns `ErrNoHealthyUpstream` for an empty candidate list.
6. `StickyByClientIP` on a `net.Conn` whose address does not parse falls back rather than panicking.

- [ ] **Step 2: Run test to verify it fails**

Run: `devbox run -- task test -- ./...`
Expected: FAIL — undefined `Selector`.

- [ ] **Step 3: Write the implementation**

```go
// Upstream is one server a port may route to.
type Upstream struct {
	Addr string
	// Weight is reserved for weighted selectors. The built-ins ignore it; it is
	// here so adding one later is not a breaking change to PortConfig.
	Weight int
}

// Selector chooses among the upstreams that passed their health check.
//
// It receives the client connection because sticky routing needs the client
// address. Nothing else uses it, and whether that is worth the parameter is the
// one API question this design left open.
type Selector interface {
	Pick(ctx context.Context, port int, up []Upstream, c net.Conn) (Upstream, error)
}
```

`RoundRobin` keeps a per-port counter in a `sync.Map` and takes the index modulo the current candidate count, so a shrinking list cannot index out of range. `LeastConn` reads `registry.UpstreamCount`, which is why the registry keeps that map. `StickyByClientIP` hashes the IP with FNV-1a from `hash/fnv` — the choice is stability across restarts, not cryptographic strength, and a stdlib hash keeps the require block empty.

Dial failover applies underneath whichever selector is configured: Task 11 marks a dial error down in the health cache and asks the selector again with the remaining candidates.

- [ ] **Step 4: Run test to verify it passes**

Run: `devbox run -- task test -- ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add selector.go selector_test.go
git commit -m "feat: add upstream selectors"
```

---

### Task 11: The proxy — listeners, accept loop, upstream resolution, Run

**Files:**
- Create: `relay.go`, `relay_test.go`

**Interfaces:**
- Consumes: every earlier task.
- Produces: `New(Config) (*Proxy, error)`, `(*Proxy).Run(ctx) error`, `(*Proxy).Shutdown(ctx) error`, `(*Proxy).Sessions()`, `(*Proxy).SessionCount()`, and `(*Proxy).Addrs() map[int]net.Addr`.

- [ ] **Step 1: Write the failing test**

Create `relay_test.go` against loopback TCP, not `net.Pipe` — this is the task where accept, bind, and shutdown are the subject. Cover:

1. **End to end.** A stub upstream echoes lines; a client connects through the proxy on a configured port and gets its line back.
2. **Port zero.** Configuring port 0 binds an ephemeral port and `Addrs()` reports it, so tests never hard-code a port.
3. **Every port is listened on, even a dead one.** A port whose only upstream is a closed address still accepts, then closes the connection once the probe comes back empty. Assert connect-then-drop, not connection-refused. This is the accepted consequence of lazy health: the earlier behaviour of never opening the listener meant a startup probe could go stale and route nothing, which is a worse failure than a visible drop.
4. **Dial failover.** Two upstreams, the first refusing connections: the session lands on the second, and the first is marked down so the next connection does not retry it within the TTL.
5. **`ErrNoHealthyUpstream`.** With every upstream down, the connection is closed and the error reaches `OnSessionError`.
6. **`MaxSessions` with `OverflowClose`.** The session after the limit is closed immediately; the count never exceeds the limit.
7. **`MaxSessions` with `OverflowWait`.** The connection waits and is served once a slot frees.
8. **Pre-frame `Handled`.** A `PreFrame` hook that reads the opening bytes, writes a reply, and returns `Handled` ends the session without any upstream being dialled. Assert the upstream saw no connection at all.
9. **Pre-frame `Continue` keeps peeked bytes.** A hook that peeks without consuming leaves the stream intact for the framer.
10. **Graceful shutdown.** `Shutdown` stops the listeners first, then gives live sessions `DrainGrace`; a session mid-relay finishes its in-flight write; `Run` returns nil.
11. **`Run` returns a fatal bind error.** Two proxies on the same fixed port: the second returns an error from `Run` rather than logging and continuing.

- [ ] **Step 2: Run test to verify it fails**

Run: `devbox run -- task test -- ./...`
Expected: FAIL — undefined `New`.

- [ ] **Step 3: Write the implementation**

`New` validates the config and builds the health cache, registry, and selector. Every configuration fault is reported here.

`Run` binds every configured port — including ports whose upstreams are all dead, since health is no longer known at startup — starts one accept goroutine per port, and blocks until the context is cancelled or a listener fails fatally. A socket and a goroutine per port cost nothing next to the staleness they replace.

The accept path, in order:

1. Acquire a session slot if `MaxSessions` is set, honouring `Overflow`.
2. Optionally wrap the connection for raw capture, so `Sink.RawChunk` sees bytes before any framing.
3. `Sink.OpenSession`.
4. Run the `PreFrame` hook against the client conduit's buffered reader — the same reader the pump will use, so bytes the hook consumes are gone and bytes it only peeks are not. `Handled` ends the session here, before any upstream is dialled.
5. Resolve the upstream: `healthCache.check` over the port's candidates, then `Selector.Pick`, then dial with `DialTimeout`. On a dial error, `markDown` and ask the selector again with the remaining candidates. When none remain, fail with `ErrNoHealthyUpstream`.
6. Build the session, register it, and run it.

`Run` returns only fatal faults — invalid configuration, a listener that cannot bind. Everything per-session goes to `OnSessionError`; with thousands of sessions there is nowhere else it can go.

- [ ] **Step 4: Run test to verify it passes**

Run: `devbox run -- task test -- ./... -count=3`
Expected: PASS. Watch for a leaked listener between runs; a test that binds port 0 and never closes will pass alone and fail in a suite.

- [ ] **Step 5: Check the gate still holds**

Run: `devbox run -- task verify`
Expected: PASS, including `deps:check`. The core is now feature-complete and its require block is still empty.

- [ ] **Step 6: Commit**

```bash
git add relay.go relay_test.go
git commit -m "feat: add the proxy, listeners, and lazy upstream resolution"
```

---

### Task 12: The typed wrappers

**Files:**
- Create: `typed/typed.go`, `typed/typed_test.go`

**Interfaces:**
- Consumes: `Hook`, `Message`, `Session`, `Action` from the core.
- Produces: `typed.Hook[P]`, `typed.On[P]`, `typed.Inject[P]`.

The core is deliberately not generic: `Message.Decoded` is `any` next to a `Descriptor`, so `Proxy`, `Session`, and `Hook` carry no type parameter and a bytes-only consumer writes no ceremony. This package is the other half of that trade — typed use should not cost hand-written assertions either.

- [ ] **Step 1: Write the failing test**

Create `typed/typed_test.go`: a `typed.On[myPacket]` hook fires only for messages whose `Decoded` is a `myPacket`, is skipped for other types and for undecoded messages, and a mutation it makes through `SetDecoded` is visible to the core as a decoded change. Assert that a non-matching message passes through as `Forward` rather than being dropped — a typed hook filters, it does not gate.

- [ ] **Step 2: Run test to verify it fails**

Run: `devbox run -- task test -- ./typed`
Expected: FAIL — no such package.

- [ ] **Step 3: Write the implementation**

```go
// Package typed removes the type assertion from a hook that only cares about
// one decoded packet type.
//
// The core carries Message.Decoded as any so that nothing in the framework has
// a type parameter and a consumer relaying opaque bytes writes no ceremony.
// That trade would be a bad one if typed use cost an assertion in every hook,
// which is what this package exists to prevent.
package typed

// On returns a relay.Hook that runs fn only when the message decoded to a P.
// Any other message, and any undecoded message, is forwarded untouched.
func On[P any](fn func(context.Context, *relay.Session, P, *relay.Message) (relay.Action, error)) relay.Hook
```

- [ ] **Step 4: Run test to verify it passes**

Run: `devbox run -- task test -- ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add typed/
git commit -m "feat: add generic hook wrappers over the untyped core"
```

---
### Task 13: The cipher example — `Transform`'s worked consumer

**Files:**
- Create: `examples/go.mod`, `examples/cipher/proxy.go`, `examples/cipher/proxy_test.go`
- Modify: `Taskfile.yml`, to add `test:examples` to `verify`

**Interfaces:**
- Consumes: `relay.Transform`, `relay.Session.Swap`, `relay.Framer`, `relay.Hook`.
- Produces: a runnable proxy that installs a real AES-CTR keystream mid-session, and the example module every later task builds in.

`Transform` needs a worked consumer and the Minecraft example cannot be one. That example's compression is a framing concern, not a stream transform, and its cipher is negotiated inside a login the example deliberately does not stand between. Demonstrating `Transform` with an identity function would be worse than not demonstrating it, so it gets its own example: small, real, and about one thing.

This example depends on nothing outside the standard library — `crypto/aes` and `crypto/cipher` are all it needs. It lives in the examples module anyway, because the core's `go.mod` stays empty and `examples/` is where anything runnable belongs.

- [ ] **Step 1: Create the example module**

```bash
cd /home/ocharnyshevich/pet.projects/go-theft-craft/relay/examples
printf 'module github.com/go-theft-craft/relay/examples\n\ngo 1.26.6\n' > go.mod
```

Add a `replace github.com/go-theft-craft/relay => ../` line. The replace belongs here and only here; `task deps:check` catches it if it drifts into the core.

Then add `test:examples` to the `verify` task in the root `Taskfile.yml`, which Task 1 deliberately left out until this module existed.

- [ ] **Step 2: Write the failing test**

Create `examples/cipher/proxy_test.go`. The protocol under test is deliberately trivial — newline-delimited lines, with one message, `START-CIPHER`, as the negotiation trigger — so that everything the test proves is about the swap and nothing is about parsing.

Assert, in this order:

1. **Plaintext before the boundary.** A line sent before the trigger arrives at the upstream unchanged, and a `net.Conn` tapping the wire sees it in the clear.
2. **The trigger crosses in the clear.** Both endpoints must agree on which message is the last unenciphered one, so the trigger itself is not enciphered — the same rule a real endpoint follows.
3. **Ciphertext after the boundary.** A line sent after the trigger is unreadable on the wire, and arrives at the upstream as the original plaintext. Assert both halves: that the wire bytes differ from the plaintext, and that the upstream got the plaintext. Only asserting the second would pass for a proxy that never enciphered anything.
4. **Both directions.** The upstream's reply after the boundary is enciphered on its own stream and arrives at the client as plaintext. This is the cross-direction swap, and it is the part the design exists for.
5. **A long session stays synchronised.** Several hundred messages after the boundary, in both directions, all arriving intact. A keystream that restarts, or a swap applied one message early, corrupts everything downstream of it rather than one message — so a single-message assertion would miss it.
6. **Swapping with bytes buffered is refused.** Send the trigger and a following message in one write, and assert the hook's `Swap` returns `ErrSwapPending` rather than silently corrupting the stream.

- [ ] **Step 3: Run test to verify it fails**

Run: `devbox run -- task test:examples`
Expected: FAIL — no package `cipher`.

- [ ] **Step 4: Write the example**

Create `examples/cipher/proxy.go`.

```go
// Package cipher is the worked example for relay.Transform: a proxy that
// relays in the clear, then switches both directions onto an AES-CTR keystream
// partway through the session.
//
// The protocol is newline-delimited lines and the key is a constant, because
// neither is the point. The point is the shape of the swap, which is the same
// shape every real mid-stream cipher needs: a trigger message that crosses in
// the clear, a swap applied to the direction the hook is running on, and a
// second swap applied to the other direction whose pump is parked in a read at
// that moment.
package cipher
```

The hook:

```go
// onTrigger installs the keystream when it sees the negotiation message.
//
// The trigger is forwarded before either swap, and forwarded in the clear. Both
// endpoints have to agree on which message is the last unenciphered one, and
// the only message both can name is the trigger itself.
//
// The ToClient swap happens while that direction's read pump is parked inside a
// socket read. That is safe, and it is the whole reason the conduit transforms
// bytes as it hands them out rather than as it buffers them; see relay.Conduit.
func onTrigger(ctx context.Context, s *relay.Session, m *relay.Message) (relay.Action, error)
```

Two things to get right, both of which the test checks:

1. **A stream cipher needs one `cipher.Stream` per direction per stream.** AES-CTR keystreams are position-dependent, so the client-side and upstream-side streams cannot share one. Four in total: encrypt and decrypt on each of the two connections. Sharing any of them produces a proxy that works for exactly one message.
2. **Forward the trigger, then swap.** Swapping first encipher's the trigger and the peer, still in the clear, cannot read it.

`Transform.Read` transforms in place, which suits `cipher.Stream.XORKeyStream` exactly — it accepts the same slice as source and destination. `Transform.Write` returns a new slice, so allocate there rather than XORing the caller's buffer; the caller still owns it and a hook or sink may hold a view.

- [ ] **Step 5: Run tests to verify they pass**

Run: `devbox run -- task test:examples -- -count=5`
Expected: PASS on every run. Run it repeatedly: a swap that races the opposite pump fails intermittently, which is exactly the failure this example exists to prove cannot happen.

- [ ] **Step 6: Prove the test can fail**

Break the example on purpose and confirm each break is caught:

- Share one `cipher.Stream` between the two directions — the long-session assertion must fail.
- Swap before forwarding the trigger — the trigger assertion must fail.
- Skip the `ToClient` swap — the cross-direction assertion must fail.

Restore afterwards. A test that has never failed has proved nothing, and this one is standing in for the coverage the Minecraft example cannot provide.

- [ ] **Step 7: Check the core gate still holds**

Run: `devbox run -- task verify`
Expected: PASS. The core's require block is still empty, and now `test:examples` runs inside `verify`.

- [ ] **Step 8: Commit**

```bash
git add examples/ Taskfile.yml
git commit -m "feat(examples): demonstrate mid-stream transforms with an AES-CTR proxy"
```

---

### Task 14: The Minecraft example — framer, codec, and prober

**Files:**
- Create: `examples/minecraft/framer.go`, `examples/minecraft/codec.go`, `examples/minecraft/prober.go`, `examples/minecraft/framer_test.go`, `examples/minecraft/codec_test.go`
- Modify: `examples/go.mod`, to add the `minecraft-protocol` requirement

**Interfaces:**
- Consumes: `relay.Framer`, `relay.Codec`, `relay.Prober`, `relay.Reader`, `relaytest.FramerContract`.
- Produces: `minecraft.Framer`, `minecraft.Codec`, `minecraft.Prober`.

This is where the seam gets tested against a protocol that was not designed for it. Everything here depends on `minecraft-protocol`; nothing here is importable from the core.

The API this task builds on, confirmed against the current tree:

| Need | Symbol |
| --- | --- |
| limits | `protocol.NewLimits(...LimitOption) (Limits, error)` |
| framing | `java.NewFramer(protocol.Limits) (protocol.Framer, error)` — `ReadFrame(io.Reader) (protocol.Frame, error)`, `BuildFrame([]byte) (protocol.Frame, error)`, `WriteFrame(io.Writer, protocol.Frame) error` |
| frame bytes | `protocol.Frame.Payload() []byte`, `.WireBytes() []byte` |
| version | `protocols.Default() protocol.Protocol`, `protocols.Resolve(id) (protocol.Protocol, bool)` |
| codec | `protocol.Protocol.NewSession(Role, Limits) (Session, error)`, `Session.SetState(State)`, `Session.DecodeFrame([]byte) (Packet, error)`, `Session.EncodeFrame(Packet) ([]byte, error)` |
| packet | `protocol.Packet{State, Direction, ID int32, Name string, Value any, Payload []byte}` |
| status ping | `protocol.NewStream(Session, Transport, ...StreamOption)`, `(*Stream).Start/Read/Write`, `protocols.Handshake(Protocol, host, port, nextState) (Packet, error)`, `protocols.StatusResponse` |

- [ ] **Step 1: Add the dependency**

Task 13 created `examples/go.mod` and wired `test:examples` into `verify`. This task only adds what the Minecraft example needs:

```bash
cd /home/ocharnyshevich/pet.projects/go-theft-craft/relay/examples
go get github.com/go-theft-craft/minecraft-protocol@latest
```

This is the first non-stdlib dependency in the repository. It belongs to the examples module; run `devbox run -- task deps:check` immediately afterwards to confirm the core is untouched.

- [ ] **Step 2: Write the failing framer test**

Create `examples/minecraft/framer_test.go` calling `relaytest.FramerContract` with the real framer and a table of real payloads: a single byte, a packet with a multi-byte varint length, one that crosses the read buffer size, and one at the configured limit. Add a case asserting a payload one byte over the limit is a read error rather than a truncated message.

This is the conformance harness earning its place. It was written in Task 3 against a newline framer; this is the first time it meets a length-prefixed one.

- [ ] **Step 3: Run test to verify it fails**

Run: `devbox run -- task test:examples`
Expected: FAIL — no package `minecraft`.

- [ ] **Step 4: Write the framer**

Create `examples/minecraft/framer.go`. A thin adapter, and the doc comment should say so — the interesting content is `wire/java`, not this file.

```go
// Framer adapts the Java edition frame envelope to relay.Framer.
//
// A relay message is one frame payload: the length prefix is framing and
// belongs here, and everything inside it is the codec's problem.
type Framer struct {
	inner  protocol.Framer
	limits protocol.Limits
}

func NewFramer(limits protocol.Limits) (*Framer, error)

func (f *Framer) ReadMessage(r relay.Reader) ([]byte, error)
func (f *Framer) WriteMessage(w io.Writer, raw []byte) error
```

Two things to get right, both of which the harness checks:

1. **Copy the payload before returning it.** `Frame.Payload()` may be a view into the frame's wire buffer, and `relay.Framer` promises the caller a slice the framer will not reuse. `bufferIsOwned` in the harness exists for exactly this; if it passes without a copy, confirm by reading `wire/java/frame.go` rather than assuming.
2. **`io.EOF` must survive.** The relay reads a clean peer close as `io.EOF` and anything else as a fault, so an EOF from `ReadFrame` at a frame boundary has to come back unwrapped, while an EOF *mid-frame* must not — that one is a truncated message.

`WriteMessage` is `BuildFrame` then `WriteFrame`. Do not write the payload directly; the length prefix is not the framer's to guess.

- [ ] **Step 5: Write the failing codec test**

Create `examples/minecraft/codec_test.go`: a handshake packet round-trips through `Decode` and `Encode` and comes back byte-identical; `Decode` returns a descriptor whose `ID` and `Name` are non-zero; a handshake with `nextState=1` moves the *serverbound* decoder into the status state and leaves the clientbound one alone; a body that decodes to nothing returns an error rather than a nil packet.

- [ ] **Step 6: Write the codec**

Create `examples/minecraft/codec.go`. The design problem here is the one the file table calls "per-direction state", and it is worth stating plainly in the doc comment:

```go
// Codec decodes frame payloads into typed packets.
//
// It holds two protocol sessions, not one. A protocol.Session fixes its inbound
// direction from the role it was built with, and a proxy reads both directions,
// so a single session could only ever decode half the traffic. The serverbound
// session is built with RoleServer because the proxy is what the client is
// talking to; the clientbound session is built with RoleClient for the mirror
// reason.
//
// Connection state is per direction for the same reason it is per session on a
// real endpoint: a handshake moves the serverbound decoder into status or
// login, and the clientbound decoder follows only when the server's reply says
// it should. Advancing both on one packet is the bug this structure exists to
// prevent.
type Codec struct {
	toServer protocol.Session
	toClient protocol.Session
}
```

`Decode(dir, raw)` picks the session by direction, calls `DecodeFrame`, and returns `(pkt.Value, relay.Descriptor{ID: pkt.ID, Name: pkt.Name}, nil)`. `Encode(value)` needs the packet identity back, so carry the whole `protocol.Packet` as `Message.Decoded` rather than just `Value` — a hook that wants the value reads one field, and the alternative is a second dispatch to recover an ID the decoder already had.

State transitions: watch for the handshake packet on the serverbound side and call `SetState` on both sessions at the point the protocol says each one changes. Write this as an explicit, commented state machine — it is short, and a reader tracing a bug here will thank you for not hiding it in a helper.

**Stop decoding at encryption.** Once a session enables encryption, decode nothing and return an error so the relay falls back to opaque passthrough. Standing between an encrypted login as a third party means running two key exchanges and holding the client's session credentials, which is a project in itself and teaches nothing about the framework seam. Say that in the comment; a reader will otherwise assume it was an oversight.

- [ ] **Step 7: Write the prober**

Create `examples/minecraft/prober.go`.

```go
// Prober reports an upstream healthy only when it answers a status request.
//
// This is the second place, after Framer, where the example shows the seam is
// real. The core's default probe is a TCP dial, which can only tell that
// something holds the port open — a server wedged before its listener accepts
// protocol traffic passes it. This one gets an answer or reports nothing.
type Prober struct {
	Descriptor protocol.Protocol
	Timeout    time.Duration
}

func (p Prober) Probe(ctx context.Context, addr string) error
```

Dial with the context, build a client session and a `protocol.Stream` over a `protocol.Transport{Reader: conn, Writer: conn, Interrupt: conn.Close}`, write `protocols.Handshake(p.Descriptor, host, port, 1)`, write the empty status request, read the response, and return nil. Close the connection on every path. `cmd/mcproto/status.go` in `minecraft-protocol` is the reference flow; follow it rather than reconstructing it.

The whole probe must respect `ctx`, because it runs on the accept path — a probe that ignores its deadline turns one wedged upstream into a stall on every connection.

- [ ] **Step 8: Run tests to verify they pass**

Run: `devbox run -- task test:examples`
Expected: PASS.

- [ ] **Step 9: Check the core gate still holds**

Run: `devbox run -- task verify`
Expected: PASS. The example module now depends on `minecraft-protocol`, and the core's require block must still be empty. This is the check the whole two-module split exists to make, so run it here rather than at the end.

- [ ] **Step 10: Commit**

```bash
git add examples/ Taskfile.yml
git commit -m "feat(examples): implement the framer, codec, and prober seams"
```

---

### Task 15: The Minecraft example's SQLite sink and runnable main

**Files:**
- Create: `examples/minecraft/store/sqlite.go`, `examples/minecraft/store/sqlite_test.go`, `examples/minecraft/main.go`

**Interfaces:**
- Consumes: `relay.Sink`, `relay.SessionInfo`, `relay.MessageRecord`, and everything from Task 14.
- Produces: `store.SQLite` and a runnable proxy.

The sink is a package rather than a file because several hundred lines of SQL and batching in the same package as the framer would bury what a reader came for.

- [ ] **Step 1: Write the failing sink test**

Create `examples/minecraft/store/sqlite_test.go` against a `t.TempDir()` database:

1. `OpenSession` returns an id, and the row exists with the addresses and open time it was given.
2. `Message` rows land with the right session id, direction, packet id, and name, and in the order they were submitted.
3. **`Message` does not block.** Submit far more messages than the batch size from a single goroutine with the writer goroutine stalled on a channel, and assert the submitting goroutine returns promptly. This is the contract the core depends on and the one a sink is most likely to break.
4. **The borrowed buffer is copied.** Submit a record, mutate the caller's `Raw` slice immediately afterwards, then assert the stored row holds the original bytes. `MessageRecord.Raw` is borrowed for the duration of the call, like everywhere else a message crosses an interface.
5. `CloseSession` followed by `Close` flushes everything — no row is lost to a batch that never filled.
6. Two concurrent sessions interleave without their rows crossing.

- [ ] **Step 2: Run test to verify it fails**

Run: `devbox run -- task test:examples`
Expected: FAIL — no package `store`.

- [ ] **Step 3: Write the sink**

Create `examples/minecraft/store/sqlite.go` over `modernc.org/sqlite`, which is chosen because it is pure Go — the example must build without cgo, or it stops being something a reader can run.

```go
// SQLite records sessions and messages to a local database.
//
// Every write goes through a buffered channel to one writer goroutine, which
// batches into a transaction and commits on a full batch or a tick. The core's
// Sink contract forbids blocking, and this is why the contract is the sink's
// problem rather than the framework's: only the implementation knows how deep
// its queue should be for the storage behind it, and a core that owned this
// goroutine could not size it for anyone.
//
// When the queue is full the sink drops records and counts the drops. Dropping
// is the right failure for a recorder: stalling a read pump to preserve a log
// line propagates backpressure to the client and turns an observability problem
// into a relaying problem.
type SQLite struct { ... }

func Open(path string, options ...Option) (*SQLite, error)
func (s *SQLite) Close() error
func (s *SQLite) Dropped() uint64
```

Schema: a `sessions` table keyed by an autoincrement id with client address, upstream address, port, opened-at, closed-at; a `messages` table with the session id, direction, packet id, name, raw blob, decoded summary, and timestamp. Index `messages(session_id, id)` — every query a reader will write starts there.

Set `PRAGMA journal_mode=WAL` and `PRAGMA synchronous=NORMAL`. A capture sink that fsyncs per commit will be the slowest thing in the process by an order of magnitude, and this is a recorder, not a ledger.

`Dropped()` is exported because a silent drop count is not an observability story. The `main` in the next step reports it on shutdown.

- [ ] **Step 4: Write the runnable proxy**

Create `examples/minecraft/main.go`: flags for listen ports, upstream addresses, protocol version, database path, and log level; wire the framer, codec, prober, and sink into a `relay.Config`; install one demonstration hook; run until SIGINT and shut down gracefully, reporting session count and dropped records.

Keep the wiring visible in one function. This file is the example's front page and someone will read it before anything else in the repository, so favour a flat sequence over helpers that make it shorter and harder to follow.

The demonstration hook should show `SetDecoded` causing a re-encode, and log the descriptor of each packet — the two things a reader wants to see the shape of.

Do not demonstrate `Session.Swap` here. This example stops decoding at encryption, so any swap it could show would be an identity function standing in for a cipher it does not implement, and a stub on the example's front page teaches the wrong shape. `Transform` has its own example in `examples/cipher` (Task 13); point at it from a comment here instead, at the place encryption is detected.

- [ ] **Step 5: Run it**

```bash
devbox run -- task build
```

Then run the binary against a real server if one is available, and confirm rows land. If no server is available, say so and rely on Task 16's end-to-end test — do not report this step as verified when it was skipped.

- [ ] **Step 6: Commit**

```bash
git add examples/
git commit -m "feat(examples): add the batched SQLite sink and a runnable proxy"
```

---
### Task 16: End-to-end proof, documentation, and the first tag

**Files:**
- Create: `examples/minecraft/proxy_test.go`
- Modify: `README.md`, `doc.go`, `docs/2026-08-16-relay-proxy-framework-design.md`, `CHANGELOG.md`

**Interfaces:**
- Consumes: everything.
- Produces: the test that proves the seam holds against a real protocol, and a repository someone else can pick up.

- [ ] **Step 1: Write the end-to-end test**

Create `examples/minecraft/proxy_test.go`. A stub upstream on loopback speaks just enough of the protocol to answer a handshake and a status request; the proxy sits in front of it with the framer, codec, prober, and the SQLite sink all wired; a client connects through the proxy and completes a status exchange. Assert:

1. The client got the stub's status response back, unchanged.
2. The sink recorded one session with the right client and upstream addresses.
3. The recorded messages carry non-zero descriptors — this is what proves the codec ran, and it is the assertion the whole example exists to make.
4. A hook that rewrites a field through `SetDecoded` changes what the client receives, and the sink records the rewritten value rather than the original.
5. `CloseSession` ran, and the row count is stable after the proxy shuts down — which is also the test that the sink's batching actually flushes on close rather than on a timer that the test outran.

Use `t.TempDir()` for the database and port 0 for every listener.

- [ ] **Step 2: Run the example tests**

Run: `devbox run -- task test:examples`
Expected: PASS.

- [ ] **Step 3: Check the conformance harness has teeth**

Task 14 ran `relaytest.FramerContract` against the real framer and it passed. A harness that passes is not yet evidence it can fail, so break the framer on purpose — drop the length check, or return `Frame.Payload()` without copying — and confirm the harness catches each one. Restore the framer afterwards.

If either mutation passes, the harness is too weak and this is the moment to strengthen it, while the only consumer is one we control.

- [ ] **Step 4: Write the README**

Rewrite `README.md` around what a reader needs in order:

- What the module does, in one paragraph, matching `doc.go`.
- The smallest working proxy: a `Framer` and a `Config` with one port, in about twenty lines.
- The seam: `Framer` required, `Codec`, `Prober`, `Sink`, `Hook`, `PreFrame` optional, one sentence each on what adding it buys.
- Mid-stream transforms, with the compression-then-encryption case named, because that is the question a reader arrives with and does not expect to be answered.
- The zero-dependency guarantee and the CI check that enforces it.
- A pointer to `examples/minecraft` as the worked example and to `relaytest` as the thing to run against your own framer.

No badges, no roadmap, no feature table.

- [ ] **Step 5: Reconcile the spec with what was built**

Read `docs/2026-08-16-relay-proxy-framework-design.md` against the code and fix every place they disagree. The spec is now a record of a design that exists, not a proposal, so update its status line and resolve its open questions with what the example actually showed:

- Does `Selector` need the `net.Conn`? Only `StickyByClientIP` used it. Record the answer either way.
- Did `Sink.RawChunk` earn its place once capture was wired through a real consumer? If the example never called it, say so rather than leaving it as an open question a reader has to re-derive.

Confirm the spec's account of `Transform` matches what got built. Two claims in particular were corrected while planning and must not drift back:

- **Compression is not a `Transform`.** It compresses each packet independently inside the frame envelope, so nothing carries between frames and it belongs to the `Framer`. Only a stream cipher, whose keystream is continuous, needs the conduit. If the spec anywhere groups the two, fix it.
- **`Transform`'s worked consumer is `examples/cipher`, not the Minecraft example.** Say why in one sentence, so the next reader does not decide the Minecraft example is missing something and add a stub to it.

- [ ] **Step 6: Cross-reference the panic divergence**

The spec commits to documenting the panic-recovery divergence in both repositories. `relay` recovers hook panics at the session boundary; `minecraft-protocol/router` deliberately does not recover handler panics. Add a sentence to `relay`'s `callHook` doc comment naming the divergence and its reason, and open a note for the corresponding sentence in `router` — that edit belongs to `minecraft-protocol` and is out of scope here, so record it rather than making it.

- [ ] **Step 7: Write the changelog and tag**

Create `CHANGELOG.md` with a `0.1.0` entry describing the initial API. Then:

```bash
devbox run -- task verify
devbox run -- task test:examples
```

Both must pass. Commit, then tag:

```bash
git tag v0.1.0
```

Do not push the tag until the repository has a remote and the module path resolves. Publishing to a public Go module path is not reversible — the proxy and the checksum database keep serving whatever is tagged, and rewriting history on GitHub does not recall it.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "docs: document the API and record the design as built"
```

---

## After this plan

Neither existing proxy is migrated by this work, deliberately. The API has now been proven against one real protocol; migrating a consumer is the next thing that will find its gaps, and that work belongs to the repositories that own those proxies.

Two things are worth watching on the first migration:

- Whether `Transform` composing in place is enough for a protocol that needs to *remove* a layer as well as add one. Nothing here supports un-swapping, because nothing needed it yet. `examples/cipher` only ever adds.
- Whether the `Sink` no-blocking contract holds up when a consumer wires a sink with real storage behind it. The contract is stated; it is not enforced, and a sink that blocks stalls a read pump and, through backpressure, its peer.
