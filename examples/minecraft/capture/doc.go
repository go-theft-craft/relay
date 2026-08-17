// Package capture records a proxied session to a file that can be replayed.
//
// It exists to be an oracle. The simulation kernel is judged correct when a
// replayed trace matches what a vanilla server actually did, so a recording is
// evidence and is treated like evidence: it records what happened and nothing
// else. It never synthesises a packet, never repairs a malformed one, and never
// reorders. A recording that will not replay is a finding to investigate, not a
// defect to smooth over — smoothing it over is how an oracle starts agreeing
// with the thing it is supposed to check.
//
// Recording belongs to the proxy rather than to either endpoint. A client and a
// server each see only their own half, and our server is one of the things
// being judged. The same binary therefore serves two uses: in front of a
// vanilla server it produces the traces later milestones are verified against,
// and in front of our own server it is the packet log that server never has to
// grow.
//
// Recordings hold player UUIDs, usernames, and chat. They are local runtime
// data and are never committed; the repository's .gitignore keeps them out.
package capture
