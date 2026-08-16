package relay

// Session is one client connection and the upstream it was joined to.
//
// It is passed to every hook and sink call, and is the handle a consumer uses
// to inject messages, attach metadata, swap a mid-stream transform, or end the
// connection.
type Session struct{}
