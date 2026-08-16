package relay

import (
	"errors"
	"testing"
	"time"
)

func minimalConfig() Config {
	return Config{
		Framer: lineFramer{},
		Ports: []PortConfig{
			{Port: 25565, Upstreams: []Upstream{{Addr: "127.0.0.1:1"}}},
		},
	}
}

func TestConfigRejectsFaults(t *testing.T) {
	cases := map[string]func(*Config){
		"no ports":          func(c *Config) { c.Ports = nil },
		"no upstreams":      func(c *Config) { c.Ports[0].Upstreams = nil },
		"no framer":         func(c *Config) { c.Framer = nil },
		"empty address":     func(c *Config) { c.Ports[0].Upstreams[0].Addr = "" },
		"port out of range": func(c *Config) { c.Ports[0].Port = 70000 },
		"negative sessions": func(c *Config) { c.MaxSessions = -1 },
		"duplicate port": func(c *Config) {
			c.Ports = append(c.Ports, PortConfig{Port: 25565, Upstreams: []Upstream{{Addr: "127.0.0.1:2"}}})
		},
	}

	for name, breakIt := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := minimalConfig()
			breakIt(&cfg)

			err := cfg.validate()
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("validate() = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

// TestConfigAllowsRepeatedEphemeralPorts exists because port 0 is a request for
// "any free port", so two of them are two different listeners rather than the
// duplicate the check is looking for.
func TestConfigAllowsRepeatedEphemeralPorts(t *testing.T) {
	cfg := minimalConfig()
	cfg.Ports = []PortConfig{
		{Port: 0, Upstreams: []Upstream{{Addr: "127.0.0.1:1"}}},
		{Port: 0, Upstreams: []Upstream{{Addr: "127.0.0.1:2"}}},
	}

	if err := cfg.validate(); err != nil {
		t.Fatalf("validate() with two ephemeral ports: %v", err)
	}
}

func TestConfigFillsDefaults(t *testing.T) {
	cfg := minimalConfig()

	if err := cfg.validate(); err != nil {
		t.Fatalf("validate(): %v", err)
	}

	if _, ok := cfg.Prober.(DialProber); !ok {
		t.Fatalf("Prober = %T, want DialProber", cfg.Prober)
	}
	if _, ok := cfg.Sink.(nopSink); !ok {
		t.Fatalf("Sink = %T, want nopSink", cfg.Sink)
	}
	if cfg.Selector == nil {
		t.Fatal("Selector was left nil")
	}
	if cfg.Logger == nil {
		t.Fatal("Logger was left nil")
	}
	if cfg.now == nil {
		t.Fatal("the clock was left nil")
	}
	if cfg.OnSessionError == nil {
		t.Fatal("OnSessionError was left nil")
	}

	numbers := []struct {
		name string
		got  int
		want int
	}{
		{"ReadBufferSize", cfg.ReadBufferSize, 4096},
		{"MaxMessageSize", cfg.MaxMessageSize, 2 << 20},
		{"MaxSessions", cfg.MaxSessions, 0},
	}
	for _, n := range numbers {
		if n.got != n.want {
			t.Errorf("%s = %d, want %d", n.name, n.got, n.want)
		}
	}

	durations := []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"ProbeTTL", cfg.ProbeTTL, 10 * time.Second},
		{"ProbeTimeout", cfg.ProbeTimeout, 2 * time.Second},
		{"DialTimeout", cfg.DialTimeout, 5 * time.Second},
		{"DrainGrace", cfg.DrainGrace, 5 * time.Second},
	}
	for _, d := range durations {
		if d.got != d.want {
			t.Errorf("%s = %v, want %v", d.name, d.got, d.want)
		}
	}

	if cfg.Overflow != OverflowClose {
		t.Errorf("Overflow = %v, want OverflowClose", cfg.Overflow)
	}
}

// TestConfigKeepsExplicitValues proves validate fills gaps rather than
// overwriting decisions the caller already made.
func TestConfigKeepsExplicitValues(t *testing.T) {
	cfg := minimalConfig()
	cfg.ReadBufferSize = 64
	cfg.MaxMessageSize = 128
	cfg.MaxSessions = 7
	cfg.Overflow = OverflowWait
	cfg.ProbeTTL = time.Minute

	if err := cfg.validate(); err != nil {
		t.Fatalf("validate(): %v", err)
	}

	if cfg.ReadBufferSize != 64 || cfg.MaxMessageSize != 128 || cfg.MaxSessions != 7 {
		t.Fatalf("validate overwrote explicit sizes: %+v", cfg)
	}
	if cfg.Overflow != OverflowWait || cfg.ProbeTTL != time.Minute {
		t.Fatalf("validate overwrote explicit policy: %+v", cfg)
	}
}

// TestDefaultSessionErrorHandlerDoesNotPanic covers the one path a caller never
// exercises deliberately: the default handler reads the session it was given.
func TestDefaultSessionErrorHandlerDoesNotPanic(t *testing.T) {
	cfg := minimalConfig()
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate(): %v", err)
	}

	cfg.OnSessionError(&Session{Info: SessionInfo{ClientAddr: "c", UpstreamAddr: "u"}}, errors.New("boom"))
}

// TestConfigRejectsBothCodecForms exists because the two fields mean different
// lifetimes, and silently preferring one would make the other look broken.
func TestConfigRejectsBothCodecForms(t *testing.T) {
	cfg := minimalConfig()
	cfg.Codec = &countingCodec{}
	cfg.NewCodec = func() (Codec, error) { return &countingCodec{}, nil }

	if err := cfg.validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("validate() with both Codec and NewCodec = %v, want ErrInvalidConfig", err)
	}
}
