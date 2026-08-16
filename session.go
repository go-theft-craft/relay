package relay

// Session is one client connection and the upstream it was joined to.
//
// It is passed to every hook and sink call, and is the handle a consumer uses
// to inject messages, attach metadata, swap a mid-stream transform, or end the
// connection.
type Session struct {
	// ID is unique for the lifetime of the process. It is the proxy's own
	// identifier, distinct from whatever a Sink assigns.
	ID int64
	// Info describes the session as it opened. It is the same value the Sink was
	// handed, so a log line and a stored row agree.
	Info SessionInfo
}
