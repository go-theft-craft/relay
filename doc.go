// Package relay proxies a stream protocol between clients and upstream
// servers without knowing what the protocol is.
//
// A consumer supplies message boundaries through a Framer and gets a working
// proxy. Supplying a Codec additionally makes decoded packets visible to hooks
// and sinks, and supplying a Prober replaces the default TCP-dial health check
// with one that speaks the protocol. Nothing in this module imports anything
// outside the standard library, which is what makes that claim checkable
// rather than aspirational.
package relay
