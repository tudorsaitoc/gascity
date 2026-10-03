package dashboardspa

import "embed"

// distFS holds the compiled Vite bundle for the dashboard SPA. Build producers
// prepare it with scripts/dashboard-input.py; Go-only CI consumers admit the
// same-source artifact before package loading. Compiled assets are not tracked.
// The all: prefix captures dotfiles and nested asset directories under dist/.
//
//go:embed all:dist
var distFS embed.FS
