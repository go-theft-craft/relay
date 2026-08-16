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
	// ErrSwapPending reports a mid-stream transform swap attempted while bytes
	// from before the boundary were still unread. See Conduit.Swap.
	ErrSwapPending = errors.New("relay: swap with bytes still buffered")
)
