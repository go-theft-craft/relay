module github.com/go-theft-craft/relay/examples

go 1.26.6

// The examples module is where every third-party dependency lives, and the
// replace is what lets it build against the core in this working tree. It
// belongs here and only here; task deps:check catches it if it drifts into the
// core.
replace github.com/go-theft-craft/relay => ../

require github.com/go-theft-craft/relay v0.0.0-00010101000000-000000000000

require github.com/go-theft-craft/minecraft-protocol v0.2.0
