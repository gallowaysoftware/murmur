package admin_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gallowaysoftware/murmur/pkg/admin"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
)

func newAuthServer(t *testing.T, opts ...admin.Option) *httptest.Server {
	t.Helper()
	rec := metrics.NewInMemory()
	srv := admin.NewServer(rec, opts...)
	return httptest.NewServer(srv.Handler())
}

func TestAuth_OffByDefault_AllowsAnonymous(t *testing.T) {
	hs := newAuthServer(t)
	defer hs.Close()

	resp, err := http.Post(hs.URL+"/murmur.admin.v1.AdminService/Health",
		"application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("anonymous Health (no auth configured): got %d %s, want 200", resp.StatusCode, body)
	}
}

func TestAuth_StaticToken_AcceptsMatchingBearer(t *testing.T) {
	hs := newAuthServer(t, admin.WithAuthToken("ops-token-1", "ops-token-2"))
	defer hs.Close()

	for _, tok := range []string{"ops-token-1", "ops-token-2"} {
		req, _ := http.NewRequest(http.MethodPost,
			hs.URL+"/murmur.admin.v1.AdminService/Health",
			strings.NewReader("{}"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("token %q: got %d, want 200", tok, resp.StatusCode)
		}
	}
}

func TestAuth_StaticToken_RejectsBadToken(t *testing.T) {
	hs := newAuthServer(t, admin.WithAuthToken("real-token"))
	defer hs.Close()

	req, _ := http.NewRequest(http.MethodPost,
		hs.URL+"/murmur.admin.v1.AdminService/Health",
		strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer wrong-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("bad token: got %d, want 401", resp.StatusCode)
	}
	if got := resp.Header.Get("WWW-Authenticate"); !strings.Contains(got, "Bearer") {
		t.Errorf("WWW-Authenticate: got %q, want substring \"Bearer\"", got)
	}
}

func TestAuth_RejectsMissingHeader(t *testing.T) {
	hs := newAuthServer(t, admin.WithAuthToken("real-token"))
	defer hs.Close()

	resp, err := http.Post(hs.URL+"/murmur.admin.v1.AdminService/Health",
		"application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("missing auth header: got %d, want 401", resp.StatusCode)
	}
}

func TestAuth_RejectsMalformedHeader(t *testing.T) {
	hs := newAuthServer(t, admin.WithAuthToken("real-token"))
	defer hs.Close()

	for _, h := range []string{
		"real-token",   // no scheme
		"Basic abc",    // wrong scheme
		"Bearer ",      // empty token
		"Bearer\treal", // tab separator (the prefix is "bearer " exact)
	} {
		req, _ := http.NewRequest(http.MethodPost,
			hs.URL+"/murmur.admin.v1.AdminService/Health",
			strings.NewReader("{}"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", h)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("Authorization=%q: got %d, want 401", h, resp.StatusCode)
		}
	}
}

func TestAuth_StaticToken_CaseInsensitiveScheme(t *testing.T) {
	hs := newAuthServer(t, admin.WithAuthToken("ok"))
	defer hs.Close()

	for _, prefix := range []string{"Bearer ", "bearer ", "BEARER ", "BeArEr "} {
		req, _ := http.NewRequest(http.MethodPost,
			hs.URL+"/murmur.admin.v1.AdminService/Health",
			strings.NewReader("{}"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", prefix+"ok")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("prefix %q: got %d, want 200", prefix, resp.StatusCode)
		}
	}
}

func TestAuth_AllowsOPTIONSWithoutCreds(t *testing.T) {
	hs := newAuthServer(t,
		admin.WithAuthToken("real-token"),
		admin.WithAllowedOrigins("https://dashboard.example"),
	)
	defer hs.Close()

	req, _ := http.NewRequest(http.MethodOptions,
		hs.URL+"/murmur.admin.v1.AdminService/Health", nil)
	req.Header.Set("Origin", "https://dashboard.example")
	req.Header.Set("Access-Control-Request-Method", "POST")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		t.Errorf("OPTIONS preflight: got %d, want 204 or 200", resp.StatusCode)
	}
}

// recordingVerifier captures the token it was asked to verify and returns
// either nil or the configured failure error.
type recordingVerifier struct {
	gotTokens []string
	fail      error
}

func (v *recordingVerifier) Verify(_ context.Context, tok string) error {
	v.gotTokens = append(v.gotTokens, tok)
	return v.fail
}

func TestAuth_JWT_AcceptsWhenVerifierApproves(t *testing.T) {
	v := &recordingVerifier{}
	hs := newAuthServer(t, admin.WithJWTVerifier(v))
	defer hs.Close()

	req, _ := http.NewRequest(http.MethodPost,
		hs.URL+"/murmur.admin.v1.AdminService/Health",
		strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer some.jwt.token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("JWT-accepted: got %d, want 200", resp.StatusCode)
	}
	if len(v.gotTokens) != 1 || v.gotTokens[0] != "some.jwt.token" {
		t.Errorf("verifier saw: %v, want [some.jwt.token]", v.gotTokens)
	}
}

func TestAuth_JWT_RejectsWhenVerifierFails(t *testing.T) {
	v := &recordingVerifier{fail: errors.New("expired")}
	hs := newAuthServer(t, admin.WithJWTVerifier(v))
	defer hs.Close()

	req, _ := http.NewRequest(http.MethodPost,
		hs.URL+"/murmur.admin.v1.AdminService/Health",
		strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer expired.jwt")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("JWT-rejected: got %d, want 401", resp.StatusCode)
	}
}
