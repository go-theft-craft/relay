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
