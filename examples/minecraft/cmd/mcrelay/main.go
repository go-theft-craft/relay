// Command mcrelay is the runnable form of the Minecraft example: a proxy that
// listens on one or more ports, relays to one or more upstreams, decodes what
// it can, and records the lot to SQLite.
//
// The wiring is deliberately kept in one flat function. This file is the
// example's front page and someone will read it before anything else in the
// repository, so a sequence you can follow top to bottom beats helpers that
// make it shorter and harder to follow.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	protocol "github.com/go-theft-craft/minecraft-protocol"
	"github.com/go-theft-craft/minecraft-protocol/protocols"
	"github.com/go-theft-craft/minecraft-protocol/wire/java"

	"github.com/go-theft-craft/relay"
	"github.com/go-theft-craft/relay/examples/minecraft"
	capturesink "github.com/go-theft-craft/relay/examples/minecraft/capture"
	"github.com/go-theft-craft/relay/examples/minecraft/replaycheck"
	"github.com/go-theft-craft/relay/examples/minecraft/store"
	"github.com/go-theft-craft/relay/examples/minecraft/trace"
)

func main() { os.Exit(dispatch(os.Args[1:], os.Stdout, os.Stderr)) }

// dispatch routes the two offline subcommands and otherwise runs the proxy.
//
// It returns an exit code rather than an error because that is what the caller
// needs and what a test can assert on: `verify` exits non-zero on a recording
// that will not replay, which is how it is used from CI and from an agent.
func dispatch(args []string, stdout, stderr io.Writer) int {
	var err error

	switch {
	case len(args) > 0 && args[0] == "trace":
		err = runTrace(args[1:], stdout)
	case len(args) > 0 && args[0] == "verify":
		err = runVerify(args[1:], stdout)
	default:
		err = run(args)
	}

	// Asking for help is not a failure. Every flag set here parses with
	// ContinueOnError, which has already printed the usage text by the time it
	// returns ErrHelp; reporting it again as an error would tell a reader who
	// typed -h that something went wrong, and exit non-zero at a caller that
	// checks.
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}

	if err != nil {
		fmt.Fprintf(stderr, "mcrelay: %v\n", err)

		return 1
	}

	return 0
}

func run(args []string) error {
	flags := flag.NewFlagSet("mcrelay", flag.ContinueOnError)

	var (
		listen    = flags.String("listen", "25565", "comma-separated ports to listen on; 0 binds an ephemeral port")
		upstreams = flags.String("upstream", "", "comma-separated upstream addresses as host:port")
		version   = flags.String("protocol", protocols.Default().ID(), "protocol ID to speak")
		dbPath    = flags.String("db", "relay.db", "SQLite database to record to")
		logLevel  = flags.String("log", "info", "log level: debug, info, warn, error")
		drain     = flags.Duration("drain", 5*time.Second, "how long a closing session may finish an in-flight write")
		capture   = flags.Bool("capture", false, "record every raw byte of the client connection, not just decoded messages")
		record    = flags.String("record", "", "directory to write one replayable .mccap recording per session into")
	)

	flags.Usage = func() {
		fmt.Fprint(flags.Output(), relayUsage)
		flags.PrintDefaults()
	}

	if err := flags.Parse(args); err != nil {
		return err
	}

	logger, err := newLogger(*logLevel)
	if err != nil {
		return err
	}

	// There is no default upstream. Pointing a proxy at a server is a decision,
	// and one made by typing it.
	if *upstreams == "" {
		return errors.New("-upstream is required")
	}

	descriptor, known := protocols.Resolve(*version)
	if !known {
		return fmt.Errorf("unknown protocol %q; known: %s", *version, strings.Join(protocols.IDs(), ", "))
	}

	limits, err := protocol.NewLimits()
	if err != nil {
		return fmt.Errorf("limits: %w", err)
	}

	ports, err := parsePorts(*listen)
	if err != nil {
		return err
	}

	candidates, err := parseUpstreams(*upstreams)
	if err != nil {
		return err
	}

	sink, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = sink.Close() }()

	// Recording composes with the store rather than replacing it, which is the
	// point of a Sink being an interface: one connection, two things watching
	// it, neither aware of the other.
	sinks := relay.Sink(sink)
	hooks := []relay.Hook{describePackets(logger)}
	if *record != "" {
		inner, err := java.NewFramer(limits)
		if err != nil {
			return fmt.Errorf("recording framer: %w", err)
		}

		recorder, err := capturesink.NewRecorder(capturesink.Options{
			Dir:        *record,
			Descriptor: descriptor,
			Limits:     limits,
			Framer:     inner,
			OnError: func(err error) {
				logger.Error("recording", slog.Any("err", err))
			},
		})
		if err != nil {
			return err
		}

		sinks = minecraft.NewMultiSink(sink, recorder)
		// The recorder's hook goes first, because it is what lets a session that
		// outruns the disk be ended instead of recorded with a hole in it, and a
		// later hook that drops a message would keep it from ever binding.
		hooks = append([]relay.Hook{recorder.Bind()}, hooks...)
	}

	portConfigs := make([]relay.PortConfig, 0, len(ports))
	for _, port := range ports {
		portConfigs = append(portConfigs, relay.PortConfig{Port: port, Upstreams: candidates})
	}

	proxy, err := relay.New(relay.Config{
		Ports: portConfigs,
		// A codec per session, not one shared: this one holds two protocol
		// sessions and a state machine, all of which belong to one connection.
		// Sharing a single instance would have every client advancing everyone
		// else's handshake.
		NewCodec: func(session *relay.Session) (relay.Codec, error) {
			return minecraft.NewCodec(session, descriptor, limits)
		},
		// A framer per session and per direction, for the same reason and one
		// more: a framer here can stop framing, and it does so for one session
		// and one direction at a time. See minecraft.Framer.
		NewFramer: func(session *relay.Session, _ relay.Direction) (relay.Framer, error) {
			return minecraft.NewFramer(session, limits)
		},
		// Health that means the server answered, rather than that something
		// holds the port open.
		Prober:   minecraft.Prober{Descriptor: descriptor, Timeout: 3 * time.Second},
		Sink:     sinks,
		Selector: relay.FirstHealthy(),
		// Off by default: it costs a copy per socket read and write, and stores
		// the whole conversation rather than a row per message.
		CaptureRaw: *capture,
		Hooks:      hooks,
		DrainGrace: *drain,
		Logger:     logger,
	})
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info(
		"mcrelay listening",
		slog.Any("ports", ports),
		slog.String("protocol", descriptor.ID()),
		slog.String("db", *dbPath),
		slog.String("recordings", *record),
	)

	if err := proxy.Run(ctx); err != nil {
		return err
	}

	logger.Info(
		"mcrelay stopped",
		slog.Int("live_sessions", proxy.SessionCount()),
		slog.Uint64("dropped_records", sink.Dropped()),
	)

	return nil
}

// describePackets is the demonstration hook: it logs what crossed the wire.
//
// Note what it does *not* do. There is no Session.Swap here, because this
// example stops decoding at encryption and any swap it could show would be an
// identity function standing in for a cipher it does not implement. A stub on
// the example's front page teaches the wrong shape; relay.Transform has a real
// worked consumer in examples/cipher.
func describePackets(logger *slog.Logger) relay.Hook {
	return relay.HookFunc(func(_ context.Context, s *relay.Session, m *relay.Message) (relay.Action, error) {
		// Once the session is enciphered a message is a socket read rather than
		// a packet, so the fields below would report a packet identifier of zero
		// and no name for every one of them — which reads as a stream of
		// keep-alives rather than as a proxy that has stopped being able to
		// look. Asking is the same question a hook has to ask before injecting.
		if minecraft.Encrypted(s) {
			logger.Debug(
				"opaque",
				slog.Int64("session", s.ID),
				slog.String("dir", m.Dir.String()),
				slog.Int("bytes", len(m.Raw)),
			)

			return relay.Forward, nil
		}

		logger.Debug(
			"packet",
			slog.Int64("session", s.ID),
			slog.String("dir", m.Dir.String()),
			slog.Int("id", int(m.Desc.ID)),
			slog.String("name", m.Desc.Name),
			slog.Int("bytes", len(m.Raw)),
		)

		return relay.Forward, nil
	})
}

func newLogger(level string) (*slog.Logger, error) {
	var parsed slog.Level
	if err := parsed.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("log level %q: %w", level, err)
	}

	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: parsed})), nil
}

func parsePorts(list string) ([]int, error) {
	fields := strings.Split(list, ",")

	ports := make([]int, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}

		var port int
		if _, err := fmt.Sscanf(field, "%d", &port); err != nil {
			return nil, fmt.Errorf("listen port %q: %w", field, err)
		}

		ports = append(ports, port)
	}

	if len(ports) == 0 {
		return nil, errors.New("-listen named no ports")
	}

	return ports, nil
}

func parseUpstreams(list string) ([]relay.Upstream, error) {
	fields := strings.Split(list, ",")

	out := make([]relay.Upstream, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}

		out = append(out, relay.Upstream{Addr: field})
	}

	if len(out) == 0 {
		return nil, errors.New("-upstream named no addresses")
	}

	return out, nil
}

// The usage text each mode prints. It is written out rather than generated,
// because the thing a reader needs first is what the three modes are for, and
// no flag list says that.
const (
	relayUsage = `mcrelay relays Minecraft connections and records what crosses.

Usage:
  mcrelay -upstream <host:port> [-listen <ports>] [-record <dir>]
  mcrelay trace  -in <file.mccap> [-out <file.json>]
  mcrelay verify <file.mccap>...

Flags:
`

	traceUsage = `mcrelay trace extracts per-entity trajectories from a recording.

Usage:
  mcrelay trace -in <file.mccap> [-out <file.json>]

The protocol comes from the recording's own header. Output is JSON on stdout
unless -out names a file.

Flags:
`

	verifyUsage = `mcrelay verify replays recordings and reports whether they reproduce themselves.

Usage:
  mcrelay verify <file.mccap>...

Exits non-zero if any recording fails, so it can gate a capture session before
the traces are trusted.
`
)

// runTrace extracts trajectories from one recording.
func runTrace(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("trace", flag.ContinueOnError)
	in := flags.String("in", "", "recording to read")
	out := flags.String("out", "", "file to write JSON to; stdout when empty")

	flags.Usage = func() {
		fmt.Fprint(flags.Output(), traceUsage)
		flags.PrintDefaults()
	}

	if err := flags.Parse(args); err != nil {
		return err
	}
	if *in == "" {
		return errors.New("trace: -in is required")
	}

	traces, header, err := trace.ExtractFile(*in)
	if err != nil {
		return err
	}

	document := traceDocument{
		Recording: *in,
		Protocol:  header.Protocol,
		Note:      header.Note,
		Traces:    traces,
	}

	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return fmt.Errorf("trace: encode: %w", err)
	}
	encoded = append(encoded, '\n')

	if *out == "" {
		_, err = stdout.Write(encoded)

		return err
	}

	if err := os.WriteFile(*out, encoded, 0o600); err != nil {
		return fmt.Errorf("trace: write %s: %w", *out, err)
	}

	return nil
}

// traceDocument is what `trace` writes. It names the recording and the protocol
// alongside the traces, because a trajectory file that cannot say where it came
// from is not much use six months later.
type traceDocument struct {
	Recording string        `json:"recording"`
	Protocol  string        `json:"protocol"`
	Note      string        `json:"note,omitempty"`
	Traces    []trace.Trace `json:"traces"`
}

// runVerify is M9.1's gate at the command line.
func runVerify(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("verify", flag.ContinueOnError)
	flags.Usage = func() { fmt.Fprint(flags.Output(), verifyUsage) }

	if err := flags.Parse(args); err != nil {
		return err
	}

	paths := flags.Args()
	if len(paths) == 0 {
		return errors.New("verify: name at least one recording")
	}

	var failed int
	for _, path := range paths {
		result, err := replaycheck.Check(context.Background(), path)
		if err != nil {
			fmt.Fprintf(stdout, "FAIL %s: %v\n", path, err)
			failed++

			continue
		}

		status := "ok"
		if !result.OK() {
			status = "FAIL"
			failed++
		}

		fmt.Fprintf(stdout, "%s %s: %d records, %s\n", status, path, result.Records, result.Explain())
	}

	if failed > 0 {
		return fmt.Errorf("%d of %d recordings did not replay", failed, len(paths))
	}

	return nil
}
