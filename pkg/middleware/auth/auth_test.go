package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testKid      = "test-key-1"
	testRealm    = "mapreduce"
	testClientID = "mapreduce-ui"
)

type testFixture struct {
	priv    *rsa.PrivateKey
	jwksSrv *httptest.Server
	cfg     Config
}

func newFixture(t *testing.T) *testFixture {
	t.Helper()

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	jwksSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload := map[string]any{
			"keys": []map[string]any{{
				"kid": testKid,
				"kty": "RSA",
				"use": "sig",
				"n":   base64.RawURLEncoding.EncodeToString(priv.PublicKey.N.Bytes()),
				"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(priv.PublicKey.E)).Bytes()),
			}},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(payload)
	}))
	t.Cleanup(jwksSrv.Close)

	// Point KeycloakURL at the mock server so jwksURL() and issuer() resolve correctly.
	cfg := Config{
		KeycloakURL:    jwksSrv.URL,
		Realm:          testRealm,
		ClientID:       testClientID,
		JWKSRefreshTTL: time.Minute,
	}

	return &testFixture{priv: priv, jwksSrv: jwksSrv, cfg: cfg}
}

func (f *testFixture) issuer() string { return f.cfg.issuer() }

type tokenOptions struct {
	subject  string
	email    string
	username string
	roles    []string
	azp      string
	issuer   string
	expiry   time.Time
}

type tokenOpt func(*tokenOptions)

func withSubject(s string) tokenOpt  { return func(o *tokenOptions) { o.subject = s } }
func withEmail(e string) tokenOpt    { return func(o *tokenOptions) { o.email = e } }
func withRoles(r ...string) tokenOpt { return func(o *tokenOptions) { o.roles = r } }
func withAZP(azp string) tokenOpt    { return func(o *tokenOptions) { o.azp = azp } }
func withIssuer(iss string) tokenOpt { return func(o *tokenOptions) { o.issuer = iss } }
func expired() tokenOpt {
	return func(o *tokenOptions) { o.expiry = time.Now().Add(-time.Hour) }
}

func (f *testFixture) signToken(t *testing.T, opts ...tokenOpt) string {
	t.Helper()

	o := tokenOptions{
		subject:  "user-uuid-123",
		email:    "alice@mapreduce.local",
		username: "alice",
		roles:    []string{"user"},
		azp:      testClientID,
		issuer:   f.issuer(),
		expiry:   time.Now().Add(time.Hour),
	}
	for _, opt := range opts {
		opt(&o)
	}

	// Build keycloakClaims with explicit top-level fields (no embedded RegisteredClaims).
	// This matches the production struct and ensures "sub" is serialised at the top
	// level of the JWT payload — exactly as Keycloak issues it.
	claims := keycloakClaims{
		Issuer:            o.issuer,
		Subject:           o.subject, // → "sub" key in JWT payload
		ExpiresAt:         jwt.NewNumericDate(o.expiry),
		IssuedAt:          jwt.NewNumericDate(time.Now()),
		Email:             o.email,
		PreferredUsername: o.username,
		AuthorizedParty:   o.azp,
	}
	claims.RealmAccess.Roles = o.roles

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = testKid

	raw, err := tok.SignedString(f.priv)
	require.NoError(t, err)
	return raw
}

func fiberTest(t *testing.T, app *fiber.App, method, path, authHeader string) *http.Response {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	resp, err := app.Test(req)
	require.NoError(t, err)
	return resp
}

func bodyJSON(t *testing.T, r *http.Response) map[string]any {
	t.Helper()
	defer r.Body.Close()
	b, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m))
	return m
}

func TestIdentity_HasRole(t *testing.T) {
	tests := []struct {
		name  string
		roles []string
		role  string
		want  bool
	}{
		{"present role", []string{"user", "operator"}, "user", true},
		{"second role present", []string{"user", "operator"}, "operator", true},
		{"role absent", []string{"user"}, "admin", false},
		{"empty query", []string{"user"}, "", false},
		{"empty roles slice", []string{}, "user", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id := &Identity{Roles: tc.roles}
			assert.Equal(t, tc.want, id.HasRole(tc.role))
		})
	}
}

func TestIdentity_IsAdmin(t *testing.T) {
	tests := []struct {
		name  string
		roles []string
		want  bool
	}{
		{"has admin", []string{"admin"}, true},
		{"has user only", []string{"user"}, false},
		{"has both", []string{"user", "admin"}, true},
		{"empty", []string{}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, (&Identity{Roles: tc.roles}).IsAdmin())
		})
	}
}

func TestIdentity_IsUser(t *testing.T) {
	tests := []struct {
		name  string
		roles []string
		want  bool
	}{
		{"has user", []string{"user"}, true},
		{"has admin only", []string{"admin"}, false},
		{"empty", []string{}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, (&Identity{Roles: tc.roles}).IsUser())
		})
	}
}

func TestGetSetIdentity_RoundTrip(t *testing.T) {
	app := fiber.New()
	want := &Identity{Subject: "sub-1", Email: "a@b.com", Roles: []string{"user"}}

	app.Get("/", func(c fiber.Ctx) error {
		SetIdentity(c, want)
		got := GetIdentity(c)
		assert.Equal(t, want, got)
		return c.SendStatus(fiber.StatusOK)
	})

	resp := fiberTest(t, app, http.MethodGet, "/", "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestGetIdentity_ReturnsNilWhenAbsent(t *testing.T) {
	app := fiber.New()
	app.Get("/", func(c fiber.Ctx) error {
		assert.Nil(t, GetIdentity(c))
		return c.SendStatus(fiber.StatusOK)
	})
	fiberTest(t, app, http.MethodGet, "/", "")
}

func TestMustGetIdentity_PanicsWhenAbsent(t *testing.T) {
	app := fiber.New()
	app.Get("/", func(c fiber.Ctx) error {
		assert.Panics(t, func() { MustGetIdentity(c) })
		return c.SendStatus(fiber.StatusOK)
	})
	fiberTest(t, app, http.MethodGet, "/", "")
}

func TestIdentityFromContext(t *testing.T) {
	app := fiber.New()
	want := &Identity{Subject: "sub-1", Email: "a@b.com", Roles: []string{"user"}}

	app.Get("/", func(c fiber.Ctx) error {
		SetIdentity(c, want)
		got := IdentityFromContext(c)
		assert.Equal(t, want, got)
		return c.SendStatus(fiber.StatusOK)
	})

	resp := fiberTest(t, app, http.MethodGet, "/", "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestIdentityFromContext_ReturnsNilWhenAbsent(t *testing.T) {
	app := fiber.New()
	app.Get("/", func(c fiber.Ctx) error {
		assert.Nil(t, IdentityFromContext(c))
		return c.SendStatus(fiber.StatusOK)
	})
	fiberTest(t, app, http.MethodGet, "/", "")
}

func TestFilterRoles(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{
			name: "removes all system roles",
			in:   []string{"user", "offline_access", "admin", "uma_authorization", "default-roles-mapreduce"},
			want: []string{"user", "admin"},
		},
		{
			name: "nil input",
			in:   nil,
			want: []string{},
		},
		{
			name: "empty input",
			in:   []string{},
			want: []string{},
		},
		{
			name: "only system roles",
			in:   []string{"offline_access", "uma_authorization"},
			want: []string{},
		},
		{
			name: "no system roles",
			in:   []string{"user", "admin"},
			want: []string{"user", "admin"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.ElementsMatch(t, tc.want, filterRoles(tc.in))
		})
	}
}

func TestExtractBearer(t *testing.T) {
	tests := []struct {
		name       string
		header     string
		wantToken  string
		wantErrMsg string
	}{
		{
			name:      "valid bearer token",
			header:    "Bearer my.jwt.token",
			wantToken: "my.jwt.token",
		},
		{
			name:       "missing header",
			header:     "",
			wantErrMsg: "missing authorization header",
		},
		{
			name:       "wrong scheme",
			header:     "Basic dXNlcjpwYXNz",
			wantErrMsg: "Bearer",
		},
		{
			name:       "empty token after bearer",
			header:     "Bearer   ",
			wantErrMsg: "Bearer",
		},
		{
			name:       "bearer only no space",
			header:     "Bearer",
			wantErrMsg: "Bearer",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			app := fiber.New()
			app.Get("/", func(c fiber.Ctx) error {
				tok, err := extractBearer(c)
				if tc.wantErrMsg != "" {
					require.Error(t, err)
					assert.ErrorContains(t, err, tc.wantErrMsg)
				} else {
					require.NoError(t, err)
					assert.Equal(t, tc.wantToken, tok)
				}
				return c.SendStatus(fiber.StatusOK)
			})
			fiberTest(t, app, http.MethodGet, "/", tc.header)
		})
	}
}

func setupApp(t *testing.T, f *testFixture) *fiber.App {
	t.Helper()
	app := fiber.New()
	app.Get("/protected", New(f.cfg), func(c fiber.Ctx) error {
		id := MustGetIdentity(c)
		return c.JSON(fiber.Map{
			"sub":     id.Subject,
			"email":   id.Email,
			"roles":   id.Roles,
			"x-sub":   c.Get("X-User-Sub"),
			"x-email": c.Get("X-User-Email"),
			"x-roles": c.Get("X-User-Roles"),
		})
	})
	return app
}

// TestNew covers the common status-code-only cases in a table, then uses
// dedicated tests for cases that need body assertions.
func TestNew(t *testing.T) {
	f := newFixture(t)
	app := setupApp(t, f)

	tests := []struct {
		name       string
		authHeader string
		wantStatus int
	}{
		{
			name:       "missing token",
			authHeader: "",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "expired token",
			authHeader: "Bearer " + f.signToken(t, expired()),
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "wrong issuer",
			authHeader: "Bearer " + f.signToken(t, withIssuer("http://evil.example.com/realms/mapreduce")),
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "wrong client ID",
			authHeader: "Bearer " + f.signToken(t, withAZP("some-other-client")),
			wantStatus: http.StatusUnauthorized,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := fiberTest(t, app, http.MethodGet, "/protected", tc.authHeader)
			assert.Equal(t, tc.wantStatus, resp.StatusCode)
		})
	}
}

func TestNew_ValidToken(t *testing.T) {
	f := newFixture(t)
	app := setupApp(t, f)

	token := f.signToken(t, withSubject("uuid-abc"), withEmail("alice@test.com"), withRoles("user"))
	resp := fiberTest(t, app, http.MethodGet, "/protected", "Bearer "+token)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := bodyJSON(t, resp)
	assert.Equal(t, "uuid-abc", body["sub"])
	assert.Equal(t, "alice@test.com", body["email"])
	assert.Equal(t, "uuid-abc", body["x-sub"], "X-User-Sub header must be forwarded")
}

func TestNew_TokenSignedWithDifferentKey(t *testing.T) {
	f := newFixture(t)
	app := setupApp(t, f)

	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	// Build claims with explicit fields (no embedded RegisteredClaims).
	claims := keycloakClaims{
		Subject:         "attacker",
		Issuer:          f.issuer(),
		IssuedAt:        jwt.NewNumericDate(time.Now()),
		ExpiresAt:       jwt.NewNumericDate(time.Now().Add(time.Hour)),
		AuthorizedParty: testClientID,
	}
	claims.RealmAccess.Roles = []string{"admin"}

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = testKid // correct kid, wrong key — signature must fail
	raw, err := tok.SignedString(otherKey)
	require.NoError(t, err)

	resp := fiberTest(t, app, http.MethodGet, "/protected", "Bearer "+raw)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestNew_SystemRolesAreFiltered(t *testing.T) {
	f := newFixture(t)
	app := setupApp(t, f)

	token := f.signToken(t, withRoles("user", "offline_access", "uma_authorization"))
	resp := fiberTest(t, app, http.MethodGet, "/protected", "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body := bodyJSON(t, resp)
	roles := body["roles"].([]any)
	assert.Contains(t, roles, "user")
	assert.NotContains(t, roles, "offline_access")
	assert.NotContains(t, roles, "uma_authorization")
}

func TestNew_EmailFallsBackToPreferredUsername(t *testing.T) {
	f := newFixture(t)
	app := fiber.New()
	app.Get("/", New(f.cfg), func(c fiber.Ctx) error {
		return c.JSON(fiber.Map{"email": MustGetIdentity(c).Email})
	})

	// Build a token with no email but a preferred_username set.
	claims := keycloakClaims{
		Subject:           "sub-1",
		Issuer:            f.issuer(),
		IssuedAt:          jwt.NewNumericDate(time.Now()),
		ExpiresAt:         jwt.NewNumericDate(time.Now().Add(time.Hour)),
		Email:             "",
		PreferredUsername: "fallback-username",
		AuthorizedParty:   testClientID,
	}
	claims.RealmAccess.Roles = []string{"user"}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = testKid
	raw, err := tok.SignedString(f.priv)
	require.NoError(t, err)

	resp := fiberTest(t, app, http.MethodGet, "/", "Bearer "+raw)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "fallback-username", bodyJSON(t, resp)["email"])
}

func TestNew_SkipsWhenNextReturnsTrue(t *testing.T) {
	f := newFixture(t)
	cfg := f.cfg
	// Skip middleware for any request to /public.
	cfg.Next = func(c fiber.Ctx) bool {
		return c.Path() == "/public"
	}

	app := fiber.New()
	app.Get("/public", New(cfg), func(c fiber.Ctx) error {
		// No token provided and no auth required because Next skips the middleware.
		return c.SendString("ok")
	})

	resp := fiberTest(t, app, http.MethodGet, "/public", "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// TestNew_EmptySubjectRejected verifies the defensive guard: a token that is
// otherwise valid but has an empty "sub" claim is rejected with 401.
func TestNew_EmptySubjectRejected(t *testing.T) {
	f := newFixture(t)
	app := setupApp(t, f)

	// Craft a token with an explicitly empty Subject.
	token := f.signToken(t, withSubject(""))
	resp := fiberTest(t, app, http.MethodGet, "/protected", "Bearer "+token)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	body := bodyJSON(t, resp)
	errMsg, _ := body["error"].(string)
	assert.Contains(t, errMsg, "sub")
}

func setupInternalApp(t *testing.T) *fiber.App {
	t.Helper()
	app := fiber.New()
	app.Get("/internal", NewInternal(), func(c fiber.Ctx) error {
		id := MustGetIdentity(c)
		return c.JSON(fiber.Map{
			"sub":   id.Subject,
			"email": id.Email,
			"roles": id.Roles,
		})
	})
	return app
}

func TestNewInternal(t *testing.T) {
	tests := []struct {
		name       string
		sub        string
		email      string
		roles      string
		wantStatus int
		check      func(t *testing.T, body map[string]any)
	}{
		{
			name:       "valid headers",
			sub:        "uuid-xyz",
			email:      "bob@test.com",
			roles:      "user,admin",
			wantStatus: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				assert.Equal(t, "uuid-xyz", body["sub"])
				assert.Equal(t, "bob@test.com", body["email"])
				roles := body["roles"].([]any)
				assert.Contains(t, roles, "user")
				assert.Contains(t, roles, "admin")
			},
		},
		{
			name:       "missing X-User-Sub",
			sub:        "",
			email:      "bob@test.com",
			roles:      "user",
			wantStatus: http.StatusUnauthorized,
			check: func(t *testing.T, body map[string]any) {
				assert.Contains(t, body["error"], "X-User-Sub")
			},
		},
		{
			name:       "empty roles header produces no roles",
			sub:        "uuid-xyz",
			email:      "",
			roles:      "",
			wantStatus: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				assert.NotContains(t, fmt.Sprintf("%v", body["roles"]), "user")
			},
		},
		{
			name:       "roles with extra spaces are trimmed",
			sub:        "uuid-xyz",
			email:      "",
			roles:      " user , admin ",
			wantStatus: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				roles := body["roles"].([]any)
				assert.Contains(t, roles, "user")
				assert.Contains(t, roles, "admin")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			app := setupInternalApp(t)

			req := httptest.NewRequest(http.MethodGet, "/internal", nil)
			if tc.sub != "" {
				req.Header.Set("X-User-Sub", tc.sub)
			}
			if tc.email != "" {
				req.Header.Set("X-User-Email", tc.email)
			}
			if tc.roles != "" {
				req.Header.Set("X-User-Roles", tc.roles)
			}

			resp, err := app.Test(req)
			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, resp.StatusCode)

			if tc.check != nil {
				tc.check(t, bodyJSON(t, resp))
			}
		})
	}
}
