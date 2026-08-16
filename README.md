# relay

```
go get github.com/go-theft-craft/relay
```

A TCP proxy framework that does not know what protocol it is proxying.

Supply message boundaries through a `Framer` and you have a working proxy: it
accepts connections on many ports, resolves an upstream per connection through
a lazily-probed health cache and a pluggable selector, and relays framed
messages in both directions. Supplying a `Codec` additionally makes decoded
packets visible to hooks and sinks, and supplying a `Prober` replaces the
default TCP-dial health check with one that speaks the protocol.

This module imports nothing outside the standard library. Its `go.mod` has no
`require` line, and CI fails if one appears — which is what makes the claim
checkable rather than aspirational.

`examples/` is a **separate module**, so every third-party dependency lives
there and none of it reaches a consumer of the core.

The design is written up in
[`docs/2026-08-16-relay-proxy-framework-design.md`](docs/2026-08-16-relay-proxy-framework-design.md).

## Status

Early. The API is still being built out against the plan in `docs/`, and
nothing is tagged yet.

## Development

```
devbox run -- task verify
```
