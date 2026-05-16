package admin

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
)

// Authenticator validates a request's credentials. Implementations are
// free to inspect any header, but the supplied helpers all read the
// HTTP Authorization header for a bearer token.
//
// The middleware calls Authenticate before the request reaches the
// Connect handler. Returning a non-nil error short-circuits the request
// with a 401 Unauthorized; a nil return passes the request through.
type Authenticator interface {
	Authenticate(r *http.Request) error
}

// JWTVerifier is the contract for OIDC / JWT validation. Wire your
// preferred library (golang-jwt, go-oidc, lestrrat-go/jwx, …) here.
// Verify receives the raw bearer token (the part after "Bearer "). A
// non-nil return is reported to the client as 401 with no body.
type JWTVerifier interface {
	Verify(ctx context.Context, token string) error
}

// ErrMissingAuth is reported when the Authorization header is absent or
// not of the form "Bearer <token>". The middleware translates this to
// 401 Unauthorized.
var ErrMissingAuth = errors.New("admin auth: missing or malformed Authorization header")

// ErrInvalidAuth is reported when the supplied bearer token doesn't
// match any configured token or fails JWT verification.
var ErrInvalidAuth = errors.New("admin auth: invalid bearer token")

// WithAuth installs an Authenticator. When set, requests that fail
// authentication get a 401 with no body before reaching the Connect
// handler. CORS preflight (OPTIONS) is always allowed through so the
// browser can complete the CORS negotiation before sending the
// credentialed request.
//
// Auth is OFF by default; the server is then suitable only for
// same-origin / network-isolated deployments. Always pair public-internet
// exposure with WithAuth or place the admin server behind an
// authenticating reverse proxy.
func WithAuth(a Authenticator) Option {
	return func(s *Server) { s.authn = a }
}

// WithAuthToken builds an Authenticator that accepts any of the supplied
// bearer tokens. Empty tokens are silently ignored (so callers can wire
// an env-var-driven list without filtering nils). Token comparison is
// constant-time, so a leaked token doesn't reveal its prefix via timing
// side channels.
//
// Use the multi-token form for rotation: deploy with the new token, wait
// for clients to roll, then drop the old.
func WithAuthToken(tokens ...string) Option {
	return WithAuth(staticTokens(tokens))
}

// WithJWTVerifier wraps a JWTVerifier as an Authenticator. Useful when
// admin tokens are issued by an OIDC IdP (Okta, Auth0, internal SSO)
// rather than hand-distributed.
func WithJWTVerifier(v JWTVerifier) Option {
	return WithAuth(jwtAuthenticator{verifier: v})
}

// staticTokens is an Authenticator backed by a fixed list of bearer
// tokens. Each Authenticate compares the request's token against every
// configured token in constant time and returns nil on first match.
type staticTokens []string

func (s staticTokens) Authenticate(r *http.Request) error {
	tok, err := bearerToken(r)
	if err != nil {
		return err
	}
	tokBytes := []byte(tok)
	for _, want := range s {
		if want == "" {
			continue
		}
		if subtle.ConstantTimeCompare(tokBytes, []byte(want)) == 1 {
			return nil
		}
	}
	return ErrInvalidAuth
}

// jwtAuthenticator forwards the bearer token to a JWTVerifier.
type jwtAuthenticator struct {
	verifier JWTVerifier
}

func (j jwtAuthenticator) Authenticate(r *http.Request) error {
	tok, err := bearerToken(r)
	if err != nil {
		return err
	}
	if err := j.verifier.Verify(r.Context(), tok); err != nil {
		return ErrInvalidAuth
	}
	return nil
}

// bearerToken extracts the token from "Authorization: Bearer <token>".
// Returns ErrMissingAuth when the header is absent, empty, or doesn't
// start with the "Bearer " prefix (case-insensitive).
func bearerToken(r *http.Request) (string, error) {
	v := r.Header.Get("Authorization")
	if v == "" {
		return "", ErrMissingAuth
	}
	const prefix = "bearer "
	if len(v) < len(prefix) || !strings.EqualFold(v[:len(prefix)], prefix) {
		return "", ErrMissingAuth
	}
	tok := strings.TrimSpace(v[len(prefix):])
	if tok == "" {
		return "", ErrMissingAuth
	}
	return tok, nil
}

// authMiddleware enforces the configured Authenticator. CORS preflight
// (OPTIONS) is always allowed through so the browser can complete the
// CORS negotiation; the actual credentialed request that follows is
// then authenticated normally.
func (s *Server) authMiddleware(next http.Handler) http.Handler {
	if s.authn == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}
		if err := s.authn.Authenticate(r); err != nil {
			// 401 without a body — never echo the supplied token or
			// internal error detail. WWW-Authenticate hint tells the
			// client what scheme to use.
			w.Header().Set("WWW-Authenticate", `Bearer realm="murmur-admin"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
