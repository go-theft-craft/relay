module github.com/go-theft-craft/relay/examples

go 1.26.6

// The examples module is where every third-party dependency lives, and the
// replace is what lets it build against the core in this working tree. It
// belongs here and only here; task deps:check catches it if it drifts into the
// core.
replace github.com/go-theft-craft/relay => ../

require github.com/go-theft-craft/relay v0.0.0-00010101000000-000000000000

require (
	github.com/go-theft-craft/minecraft-protocol v0.8.0
	modernc.org/sqlite v1.56.0
)

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	golang.org/x/sys v0.47.0 // indirect
	modernc.org/libc v1.74.4 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)
