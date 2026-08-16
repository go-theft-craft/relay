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
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	protocol "github.com/go-theft-craft/minecraft-protocol"
	"github.com/go-theft-craft/minecraft-protocol/protocols"

	"github.com/go-theft-craft/relay"
	"github.com/go-theft-craft/relay/examples/minecraft"
	"github.com/go-theft-craft/relay/examples/minecraft/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "mcrelay: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		listen    = flag.String("listen", "25565", "comma-separated ports to listen on; 0 binds an ephemeral port")
		upstreams = flag.String("upstream", "", "comma-separated upstream addresses as host:port")
		version   = flag.String("protocol", protocols.Default().ID(), "protocol ID to speak")
		dbPath    = flag.String("db", "relay.db", "SQLite database to record to")
		logLevel  = flag.String("log", "info", "log level: debug, info, warn, error")
		drain     = flag.Duration("drain", 5*time.Second, "how long a closing session may finish an in-flight write")
	)
	flag.Parse()

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

	framer, err := minecraft.NewFramer(limits)
	if err != nil {
		return err
	}

	sink, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = sink.Close() }()

	portConfigs := make([]relay.PortConfig, 0, len(ports))
	for _, port := range ports {
		portConfigs = append(portConfigs, relay.PortConfig{Port: port, Upstreams: candidates})
	}

	proxy, err := relay.New(relay.Config{
		Ports:  portConfigs,
		Framer: framer,
		// A codec per session, not one shared: this one holds two protocol
		// sessions and a state machine, all of which belong to one connection.
		// Sharing a single instance would have every client advancing everyone
		// else's handshake.
		NewCodec: func() (relay.Codec, error) {
			return minecraft.NewCodec(descriptor, limits)
		},
		// Health that means the server answered, rather than that something
		// holds the port open.
		Prober:     minecraft.Prober{Descriptor: descriptor, Timeout: 3 * time.Second},
		Sink:       sink,
		Selector:   relay.FirstHealthy(),
		Hooks:      []relay.Hook{describePackets(logger)},
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
