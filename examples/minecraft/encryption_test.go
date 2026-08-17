package minecraft_test

import (
	"bufio"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	protocol "github.com/go-theft-craft/minecraft-protocol"
	capturepkg "github.com/go-theft-craft/minecraft-protocol/capture"
	"github.com/go-theft-craft/minecraft-protocol/generated/java/v1_8"
	"github.com/go-theft-craft/minecraft-protocol/protocols"
	"github.com/go-theft-craft/minecraft-protocol/wire/java"

	"github.com/go-theft-craft/relay/examples/minecraft"
	capturesink "github.com/go-theft-craft/relay/examples/minecraft/capture"
	"github.com/go-theft-craft/relay/examples/minecraft/replaycheck"
)

// The shared secret is a constant so that every byte after the key exchange is
// the same on every run. AES-CFB8 over fixed plaintext with a fixed key is
// deterministic, and what this test is watching — where a proxy that cannot
// read the stream decides one message ends and the next begins — is decided by
// those bytes. A random secret would make the outcome a coin flip per run.
var stubSharedSecret = []byte{
	0x0f, 0x1e, 0x2d, 0x3c, 0x4b, 0x5a, 0x69, 0x78,
	0x87, 0x96, 0xa5, 0xb4, 0xc3, 0xd2, 0xe1, 0xf0,
}

var stubVerifyToken = []byte{0x11, 0x22, 0x33, 0x44}

// encryptionPacketID is the key exchange in protocol 47's login state. Both
// directions use it: the server's request and the client's answer share an
// identifier and are told apart by which way they travel.
const encryptionPacketID int32 = 0x01

// The login beside this one is offline: no server asks for a key, so the codec
// never latches and nothing here had ever run the branch that does. The
// verification document called encryption the honest remainder for exactly that
// reason — a person with a real client was the only way to see it happen.
//
// What follows is that exchange reduced to a stub, and it is not a formality.
// Three claims in this tree said the session "continues as opaque passthrough"
// after the key exchange, and the first run of this test showed that it did not:
// the codec stopped decoding as documented, but the framer went on looking for a
// length prefix, found one in ciphertext, and parked waiting for the 1.7 MB it
// announced. No error, no log line past the first, and a login that never
// finished. Take the two latch checks out of Framer.ReadMessage and this test
// fails that way again.
//
// What the round trip proves is stronger than "the bytes arrived": AES-CFB8
// feeds every byte into the next byte's keystream, so one byte added, dropped,
// or reordered anywhere in the stream turns everything after it into noise. Two
// packets decoded in one direction and one in the other is a proxy that passed
// the stream through exactly.
func TestAnOnlineModeLoginRelaysOpaquely(t *testing.T) {
	t.Parallel()

	up, _ := recordAnEncryptedLogin(t)

	select {
	case reported := <-up.walked:
		if reported != stubWalkX {
			t.Errorf("the stub read the client at x=%v, want %v", reported, stubWalkX)
		}
	case <-time.After(time.Second):
		t.Error("the stub never read the client's enciphered position")
	}
}

// TestAnOnlineModeLoginRecordsHonestly is the recorder's half of the same run.
//
// A capture of an enciphered session is a capture of a login and then nothing,
// and all three of those words have to be true in the file: the login has to be
// there, the key material must not be, and the nothing has to be marked rather
// than merely absent. The format has a record for the third — a secret record
// marks where encryption began — and until this test nothing here had ever
// written one.
func TestAnOnlineModeLoginRecordsHonestly(t *testing.T) {
	t.Parallel()

	_, path := recordAnEncryptedLogin(t)

	records := loginRecords(t, path)

	var (
		exchanges int
		secrets   int
		after     []string
	)

	for _, record := range records {
		if secrets > 0 && record.Kind != capturepkg.KindTrailer {
			after = append(after, fmt.Sprintf("%v/%s", record.Kind, record.Name))
		}

		switch {
		case record.Kind == capturepkg.KindSecret:
			secrets++

			if !record.Redacted {
				t.Error("the record marking the switch to ciphertext is not redacted")
			}
			if len(record.Payload) != 0 {
				t.Errorf("the record marking the switch carries %d bytes; the proxy never held the key", len(record.Payload))
			}
		case record.Kind == capturepkg.KindPacket && record.PacketID == encryptionPacketID &&
			record.BeforeState == protocol.State("login"):
			exchanges++

			if !record.Redacted || len(record.Payload) != 0 {
				t.Errorf("the %v key-exchange packet record kept its body; a capture must not hold key material", record.Direction)
			}

			assertItsRawFrameWasWithheld(t, records, record.Frame)
		}
	}

	// Both halves: the server's request and the client's answer. A test that
	// found one would pass against a recorder that withheld only the direction
	// it happened to look at.
	if exchanges != 2 {
		t.Errorf("the capture holds %d withheld key-exchange packet records, want 2", exchanges)
	}
	if secrets != 1 {
		t.Errorf("the capture holds %d records marking the switch to ciphertext, want 1", secrets)
	}
	if len(after) != 0 {
		t.Errorf("the capture holds %d records after the switch: %v — ciphertext is not frames and must not be filed as any",
			len(after), after)
	}

	// It still has to be a recording. A file that stops early and is complete is
	// evidence about a login; a file that stops early and does not replay is a
	// file nobody can say anything about.
	result, err := replaycheck.Check(t.Context(), path)
	if err != nil {
		t.Fatalf("replaycheck.Check: %v", err)
	}
	if !result.OK() {
		t.Errorf("the capture does not replay: %s", result.Explain())
	}
	if result.Records == 0 {
		t.Error("the capture holds no replayable records")
	}
}

// assertItsRawFrameWasWithheld follows a withheld packet record to the raw
// record beside it, because the pair is withheld together or the withholding
// means nothing: the raw record is the one a replay reads.
func assertItsRawFrameWasWithheld(t *testing.T, records []capturepkg.Record, frame uint64) {
	t.Helper()

	for _, record := range records {
		if record.Kind != capturepkg.KindRawFrame || record.Frame != frame {
			continue
		}

		if !record.Redacted || len(record.Payload) != 0 {
			t.Errorf("raw frame %d kept the bytes of a key exchange", frame)
		}

		return
	}

	t.Errorf("key-exchange frame %d has no raw record at all", frame)
}

// recordAnEncryptedLogin runs one online-mode login through the proxy and
// returns the stub that served it along with the recording it produced.
func recordAnEncryptedLogin(t *testing.T) (up *encryptedStub, path string) {
	t.Helper()

	descriptor, known := protocols.Resolve("java/1.8.9")
	if !known {
		t.Skip("protocol java/1.8.9 is not registered")
	}

	up = newEncryptedStub(t, descriptor)

	dir := t.TempDir()
	limits := loginLimits(t)

	inner, err := java.NewFramer(limits)
	if err != nil {
		t.Fatalf("NewFramer: %v", err)
	}

	recorder, err := capturesink.NewRecorder(capturesink.Options{
		Dir:        dir,
		Descriptor: descriptor,
		Limits:     limits,
		Framer:     inner,
		OnError:    func(err error) { t.Errorf("recorder reported: %v", err) },
	})
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}

	addr, idle := runLoginProxy(t, descriptor, up.addr(), recorder)

	encryptedClient(t, descriptor, addr)
	idle()

	matches, err := filepath.Glob(filepath.Join(dir, "*.mccap"))
	if err != nil {
		t.Fatalf("Glob: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("the login produced %d recordings, want 1", len(matches))
	}

	return up, matches[0]
}

// encryptedStub is an online-mode server: it asks for a key before it will
// finish a login, and speaks nothing but ciphertext afterwards.
type encryptedStub struct {
	ln     net.Listener
	key    *rsa.PrivateKey
	walked chan float64
}

func newEncryptedStub(t *testing.T, descriptor protocol.Protocol) *encryptedStub {
	t.Helper()

	// Generated per run rather than pinned, because a private key checked into
	// a repository is a finding no matter what it protects.
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	s := &encryptedStub{ln: ln, key: key, walked: make(chan float64, 1)}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			go s.serve(t, descriptor, conn)
		}
	}()

	return s
}

func (s *encryptedStub) addr() string { return s.ln.Addr().String() }

func (s *encryptedStub) serve(t *testing.T, descriptor protocol.Protocol, conn net.Conn) {
	defer func() { _ = conn.Close() }()

	limits := loginLimits(t)

	framer, err := minecraft.NewFramer(nil, limits)
	if err != nil {
		return
	}

	session, err := descriptor.NewSession(protocol.RoleServer, limits)
	if err != nil {
		return
	}

	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	reader := bufio.NewReader(conn)

	raw, err := framer.ReadMessage(reader)
	if err != nil {
		return
	}

	handshake, err := session.DecodeFrame(raw)
	if err != nil {
		return
	}

	advanceBoth(session, handshake)

	fields, ok := protocols.ReadHandshake(handshake)
	if !ok {
		return
	}

	if fields.NextState == 1 {
		serveStatus(descriptor, session, framer, conn, reader)

		return
	}

	if raw, err = framer.ReadMessage(reader); err != nil {
		return
	}

	start, err := session.DecodeFrame(raw)
	if err != nil {
		return
	}

	advanceBoth(session, start)

	public, err := x509.MarshalPKIXPublicKey(&s.key.PublicKey)
	if err != nil {
		return
	}

	if !writeLoginPacket(session, framer, conn, 0x01, protocol.State("login"), &v1_8.LoginClientboundEncryptionBegin{
		ServerID:    "",
		PublicKey:   public,
		VerifyToken: stubVerifyToken,
	}) {
		return
	}

	// The response is the last frame either peer sends in the clear.
	if raw, err = framer.ReadMessage(reader); err != nil {
		return
	}

	response, err := session.DecodeFrame(raw)
	if err != nil {
		return
	}

	advanceBoth(session, response)

	answer, ok := response.Value.(*v1_8.LoginServerboundEncryptionBegin)
	if !ok {
		return
	}

	secret, err := rsa.DecryptPKCS1v15(nil, s.key, answer.SharedSecret)
	if err != nil {
		return
	}

	token, err := rsa.DecryptPKCS1v15(nil, s.key, answer.VerifyToken)
	if err != nil || string(token) != string(stubVerifyToken) {
		return
	}

	enciphered, err := encipher(conn, secret)
	if err != nil {
		return
	}

	if !writeLoginPacket(session, framer, enciphered, 0x02, protocol.State("login"), &v1_8.LoginClientboundSuccess{
		UUID:     "d8f0a3c2-0000-4000-8000-000000000002",
		Username: "Stubbed",
	}) {
		return
	}

	if !writeLoginPacket(session, framer, enciphered, 0x08, protocol.State("play"), &v1_8.PlayClientboundPosition{
		X: stubSpawnX, Y: stubSpawnY, Z: stubSpawnZ,
	}) {
		return
	}

	cipherReader := bufio.NewReader(enciphered)

	for {
		raw, err := framer.ReadMessage(cipherReader)
		if err != nil {
			return
		}

		packet, err := session.DecodeFrame(raw)
		if err != nil {
			return
		}

		advanceBoth(session, packet)

		if look, ok := packet.Value.(*v1_8.PlayServerboundPositionLook); ok {
			select {
			case s.walked <- look.X:
			default:
			}
		}
	}
}

// encryptedClient is the online-mode client half: it answers the key request,
// switches its transport, and then reads and writes nothing but ciphertext.
func encryptedClient(t *testing.T, descriptor protocol.Protocol, addr string) {
	t.Helper()

	limits := loginLimits(t)

	framer, err := minecraft.NewFramer(nil, limits)
	if err != nil {
		t.Fatalf("NewFramer: %v", err)
	}

	session, err := descriptor.NewSession(protocol.RoleClient, limits)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))

	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split %q: %v", addr, err)
	}

	port, err := net.LookupPort("tcp", portText)
	if err != nil {
		t.Fatalf("port: %v", err)
	}

	handshake, err := protocols.Handshake(descriptor, host, uint16(port), 2)
	if err != nil {
		t.Fatalf("Handshake: %v", err)
	}

	writePacket(t, framer, session, conn, handshake)
	session.SetState(protocol.State("login"))

	writePacket(t, framer, session, conn, protocol.Packet{
		State:     protocol.State("login"),
		Direction: protocol.DirectionServerbound,
		ID:        0x00,
		Value:     &v1_8.LoginServerboundLoginStart{Username: "Stubbed"},
	})

	reader := bufio.NewReader(conn)

	raw, err := framer.ReadMessage(reader)
	if err != nil {
		t.Fatalf("read the key request: %v", err)
	}

	packet, err := session.DecodeFrame(raw)
	if err != nil {
		t.Fatalf("decode the key request: %v", err)
	}

	advanceBoth(session, packet)

	request, ok := packet.Value.(*v1_8.LoginClientboundEncryptionBegin)
	if !ok {
		t.Fatalf("the stub sent %T, want a key request", packet.Value)
	}

	parsed, err := x509.ParsePKIXPublicKey(request.PublicKey)
	if err != nil {
		t.Fatalf("parse the server key: %v", err)
	}

	public, ok := parsed.(*rsa.PublicKey)
	if !ok {
		t.Fatalf("the server key is %T, want RSA", parsed)
	}

	// PKCS #1 v1.5 rather than OAEP, which Go 1.26 deprecated it in favour of.
	// The padding is the protocol's, not a choice this test makes: a stub that
	// used OAEP would be encrypting to a scheme no Minecraft server implements,
	// and the thing under test — what a proxy does when the stream it is
	// relaying stops being readable — needs the exchange to be the real one.
	secret, err := rsa.EncryptPKCS1v15(rand.Reader, public, stubSharedSecret)
	if err != nil {
		t.Fatalf("encrypt the shared secret: %v", err)
	}

	token, err := rsa.EncryptPKCS1v15(rand.Reader, public, request.VerifyToken)
	if err != nil {
		t.Fatalf("encrypt the verify token: %v", err)
	}

	writePacket(t, framer, session, conn, protocol.Packet{
		State:     protocol.State("login"),
		Direction: protocol.DirectionServerbound,
		ID:        0x01,
		Value:     &v1_8.LoginServerboundEncryptionBegin{SharedSecret: secret, VerifyToken: token},
	})

	// Nothing may be buffered here: the stub sends nothing between the key
	// request and the answer to it, so a byte waiting at this point would be a
	// byte about to be read under the wrong encoding.
	if reader.Buffered() != 0 {
		t.Fatalf("%d bytes were buffered across the switch to ciphertext", reader.Buffered())
	}

	enciphered, err := encipher(conn, stubSharedSecret)
	if err != nil {
		t.Fatalf("encipher: %v", err)
	}

	cipherReader := bufio.NewReader(enciphered)

	var joined, placed bool

	for !joined || !placed {
		raw, err := framer.ReadMessage(cipherReader)
		if err != nil {
			t.Fatalf("read an enciphered frame (joined=%v, placed=%v): %v", joined, placed, err)
		}

		packet, err := session.DecodeFrame(raw)
		if err != nil {
			t.Fatalf("decode an enciphered frame (joined=%v, placed=%v): %v", joined, placed, err)
		}

		advanceBoth(session, packet)

		switch value := packet.Value.(type) {
		case *v1_8.LoginClientboundSuccess:
			if value.Username != "Stubbed" {
				t.Errorf("the login succeeded for %q, want Stubbed", value.Username)
			}

			joined = true
		case *v1_8.PlayClientboundPosition:
			if value.X != stubSpawnX || value.Y != stubSpawnY || value.Z != stubSpawnZ {
				t.Errorf("the client was placed at (%v, %v, %v), want (%v, %v, %v)",
					value.X, value.Y, value.Z, stubSpawnX, stubSpawnY, stubSpawnZ)
			}

			placed = true
		}
	}

	writePacket(t, framer, session, enciphered, protocol.Packet{
		State:     protocol.State("play"),
		Direction: protocol.DirectionServerbound,
		ID:        0x06,
		Value: &v1_8.PlayServerboundPositionLook{
			X: stubWalkX, Y: stubSpawnY, Z: stubSpawnZ, OnGround: true,
		},
	})

	time.Sleep(200 * time.Millisecond)
}

// encipher wraps a connection in the transport cipher the protocol switches to.
func encipher(conn net.Conn, secret []byte) (*cipherConn, error) {
	send, err := newCFB8(secret, false)
	if err != nil {
		return nil, err
	}

	recv, err := newCFB8(secret, true)
	if err != nil {
		return nil, err
	}

	return &cipherConn{
		reader: cipher.StreamReader{S: recv, R: conn},
		writer: cipher.StreamWriter{S: send, W: conn},
	}, nil
}

// cipherConn is a connection whose bytes are enciphered in both directions.
type cipherConn struct {
	reader cipher.StreamReader
	writer cipher.StreamWriter
}

func (c *cipherConn) Read(p []byte) (int, error)  { return c.reader.Read(p) }
func (c *cipherConn) Write(p []byte) (int, error) { return c.writer.Write(p) }

// cfb8 is AES in 8-bit cipher feedback, which is the mode this protocol's
// transport uses and which the standard library does not offer: crypto/cipher's
// CFB is full-block. The distinction matters here rather than being pedantry,
// because CFB8 is byte-granular — a frame's ciphertext is exactly as long as
// its plaintext and leaves as soon as it is written, which is what puts a
// framing proxy in front of a length prefix it cannot read.
//
// The shared secret is both key and initialisation vector, as it is on the
// wire.
type cfb8 struct {
	block   cipher.Block
	iv      []byte
	scratch []byte
	decrypt bool
}

func newCFB8(secret []byte, decrypt bool) (*cfb8, error) {
	block, err := aes.NewCipher(secret)
	if err != nil {
		return nil, fmt.Errorf("cfb8: %w", err)
	}

	return &cfb8{
		block:   block,
		iv:      append([]byte(nil), secret...),
		scratch: make([]byte, block.BlockSize()),
		decrypt: decrypt,
	}, nil
}

func (c *cfb8) XORKeyStream(dst, src []byte) {
	for i, in := range src {
		c.block.Encrypt(c.scratch, c.iv)

		out := in ^ c.scratch[0]

		// The feedback is always the ciphertext byte, which is the one being
		// produced when enciphering and the one being consumed when deciphering.
		feedback := out
		if c.decrypt {
			feedback = in
		}

		copy(c.iv, c.iv[1:])
		c.iv[len(c.iv)-1] = feedback

		dst[i] = out
	}
}

var _ cipher.Stream = (*cfb8)(nil)
