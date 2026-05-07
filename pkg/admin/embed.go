package admin

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed all:dist
var distFS embed.FS

// UIHandler returns an http.Handler that serves the embedded Vite build (web/dist
// copied into pkg/admin/dist by the build pipeline). Falls back to index.html for
// unknown paths so client-side routing works (refresh on /pipelines/foo).
//
// If the embedded FS is empty (build wasn't run), the handler returns 503 with a
// pointer to the build instructions.
func UIHandler() http.Handler {
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		return notBuiltHandler{}
	}
	// Detect missing dist (just the .gitkeep / no index.html).
	if _, err := fs.Stat(sub, "index.html"); err != nil {
		return notBuiltHandler{}
	}
	return spaHandler{root: sub, fs: http.FileServer(http.FS(sub))}
}

type spaHandler struct {
	root fs.FS
	fs   http.Handler
}

func (h spaHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" {
		path = "index.html"
	}
	if _, err := fs.Stat(h.root, path); err != nil {
		// Unknown route → serve index.html for client-side routing.
		r.URL.Path = "/"
	}
	h.fs.ServeHTTP(w, r)
}

type notBuiltHandler struct{}

func (notBuiltHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte(
		"Murmur UI not embedded.\n\n" +
			"Build it from the repo root:\n" +
			"  (cd web && npm install && npm run build)\n" +
			"  cp -r web/dist pkg/admin/dist\n" +
			"  go build ./cmd/murmur-ui\n",
	))
}

// FullHandler combines the admin Connect-RPC service and the embedded UI on a
// single mux. Requests under "/api/" are stripped of the prefix and forwarded
// to the AdminService handler (which expects paths of the form
// "/murmur.admin.v1.AdminService/<RPC>"); everything else is served by the
// SPA handler.
//
// The "/api" prefix is a UI-side convention so the same dist works under
// `npm run dev` (Vite proxy: /api → :8080) and in production (embedded). Other
// non-browser clients can speak directly to the Connect service at "/" by
// pointing at <host>:<port>/murmur.admin.v1.AdminService/<RPC> if the API is
// the only thing on the port.
func (s *Server) FullHandler() http.Handler {
	api := http.StripPrefix("/api", s.Handler())
	ui := UIHandler()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") {
			api.ServeHTTP(w, r)
			return
		}
		ui.ServeHTTP(w, r)
	})
}
