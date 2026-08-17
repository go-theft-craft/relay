package minecraft

import (
	"sync/atomic"

	"github.com/go-theft-craft/relay"
)

// linkKey is where a session's link hangs in relay.Session metadata. It is
// namespaced because the metadata map is shared with every other consumer of
// the session.
const linkKey = "minecraft.link"

// link is the one fact a session's codec and its two framers each know half of.
//
// The codec is the only part that can see the key exchange happen, because
// seeing it means decoding a packet. The framers are the only parts that can act
// on it, because what changes is where a message ends: an enciphered stream has
// no length prefix a third party can read, so the frame boundaries stop being
// discoverable at exactly the moment the codec stops decoding.
//
// Neither can learn it alone, so it lives beside both. relay.Config.NewFramer
// documents session metadata as the place for precisely this.
type link struct {
	// encrypted latches once and never clears, because a stream cipher does not
	// go back. It is atomic because the two framers and the codec read and write
	// it from three goroutines.
	encrypted atomic.Bool
}

// linkFor returns the session's link, creating it on first use. A nil session
// gets a private link that nothing else can reach, which is what a codec or a
// framer built outside a relay wants.
//
// It is called while a session is being built — relay calls NewCodec and then
// NewFramer for each direction, all on the accepting goroutine — so the
// get-then-set below cannot race with itself. Later callers only ever find what
// those calls left.
func linkFor(session *relay.Session) *link {
	if session == nil {
		return &link{}
	}

	if existing, ok := session.Get(linkKey); ok {
		if found, ok := existing.(*link); ok {
			return found
		}
	}

	fresh := &link{}
	session.Set(linkKey, fresh)

	return fresh
}

// Encrypted reports whether this session's key exchange has completed, which is
// the same as asking whether anything downstream can still make sense of its
// bytes.
//
// It is exported because the answer changes what a consumer's own parts should
// do, and they have no other way to find out. A recorder must stop calling what
// it is handed a frame; a hook must stop injecting, because an injected packet
// would go out in the clear onto an enciphered link and desynchronise both
// peers' ciphers.
func Encrypted(session *relay.Session) bool {
	if session == nil {
		return false
	}

	value, ok := session.Get(linkKey)
	if !ok {
		return false
	}

	found, ok := value.(*link)

	return ok && found.encrypted.Load()
}
