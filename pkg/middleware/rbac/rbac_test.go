package rbac

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/mirstar13/go-map-reduce/pkg/middleware/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ok is a trivial terminal handler that confirms the middleware chain passed.
var ok = func(c fiber.Ctx) error { return c.SendString("ok") }

// withIdentity is a Fiber middleware that pre-loads an Identity into locals,
// simulating what auth.New or auth.NewInternal would do in production.
func withIdentity(id *auth.Identity) fiber.Handler {
	return func(c fiber.Ctx) error {
		auth.SetIdentity(c, id)
		return c.Next()
	}
}

// get sends a GET request to the app and returns the response.
func get(t *testing.T, app *fiber.App, path string) *http.Response {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	return resp
}

// bodyStr reads the response body as a plain string.
func bodyStr(t *testing.T, r *http.Response) string {
	t.Helper()
	defer r.Body.Close()
	b, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	return string(b)
}

func TestRequireRole(t *testing.T) {
	tests := []struct {
		name       string
		identity   *auth.Identity // nil = unauthenticated
		role       string
		wantStatus int
	}{
		{
			name:       "passes when role matches",
			identity:   &auth.Identity{Roles: []string{"user"}},
			role:       "user",
			wantStatus: http.StatusOK,
		},
		{
			name:       "forbidden when role is absent",
			identity:   &auth.Identity{Roles: []string{"user"}},
			role:       "admin",
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "forbidden error message contains the required role",
			identity:   &auth.Identity{Roles: []string{"user"}},
			role:       "admin",
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "unauthorized when no identity",
			identity:   nil,
			role:       "user",
			wantStatus: http.StatusUnauthorized,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			app := fiber.New()
			if tc.identity != nil {
				app.Get("/", withIdentity(tc.identity), RequireRole(tc.role), ok)
			} else {
				app.Get("/", RequireRole(tc.role), ok)
			}
			resp := get(t, app, "/")
			assert.Equal(t, tc.wantStatus, resp.StatusCode)
			if tc.wantStatus == http.StatusForbidden {
				assert.Contains(t, bodyStr(t, resp), tc.role)
			}
		})
	}
}

func TestRequireAdmin(t *testing.T) {
	tests := []struct {
		name       string
		identity   *auth.Identity
		wantStatus int
	}{
		{
			name:       "passes for admin role",
			identity:   &auth.Identity{Roles: []string{"admin"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "forbidden for user-only role",
			identity:   &auth.Identity{Roles: []string{"user"}},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "passes when identity holds both roles",
			identity:   &auth.Identity{Roles: []string{"user", "admin"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "unauthorized when no identity",
			identity:   nil,
			wantStatus: http.StatusUnauthorized,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			app := fiber.New()
			if tc.identity != nil {
				app.Get("/", withIdentity(tc.identity), RequireAdmin(), ok)
			} else {
				app.Get("/", RequireAdmin(), ok)
			}
			assert.Equal(t, tc.wantStatus, get(t, app, "/").StatusCode)
		})
	}
}

func TestRequireUser(t *testing.T) {
	tests := []struct {
		name       string
		identity   *auth.Identity
		wantStatus int
	}{
		{
			name:       "passes for user role",
			identity:   &auth.Identity{Roles: []string{"user"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "forbidden for empty roles",
			identity:   &auth.Identity{Roles: []string{}},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "forbidden for admin-only identity",
			identity:   &auth.Identity{Roles: []string{"admin"}},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "unauthorized when no identity",
			identity:   nil,
			wantStatus: http.StatusUnauthorized,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			app := fiber.New()
			if tc.identity != nil {
				app.Get("/", withIdentity(tc.identity), RequireUser(), ok)
			} else {
				app.Get("/", RequireUser(), ok)
			}
			assert.Equal(t, tc.wantStatus, get(t, app, "/").StatusCode)
		})
	}
}

func TestRequireAnyRole(t *testing.T) {
	tests := []struct {
		name         string
		identity     *auth.Identity
		allowedRoles []string
		wantStatus   int
	}{
		{
			name:         "passes on first allowed role",
			identity:     &auth.Identity{Roles: []string{"operator"}},
			allowedRoles: []string{"admin", "operator"},
			wantStatus:   http.StatusOK,
		},
		{
			name:         "passes on second allowed role",
			identity:     &auth.Identity{Roles: []string{"admin"}},
			allowedRoles: []string{"operator", "admin"},
			wantStatus:   http.StatusOK,
		},
		{
			name:         "forbidden when no role matches",
			identity:     &auth.Identity{Roles: []string{"user"}},
			allowedRoles: []string{"admin", "operator"},
			wantStatus:   http.StatusForbidden,
		},
		{
			name:         "forbidden error lists all required roles",
			identity:     &auth.Identity{Roles: []string{"user"}},
			allowedRoles: []string{"admin", "operator"},
			wantStatus:   http.StatusForbidden,
		},
		{
			name:         "unauthorized when no identity",
			identity:     nil,
			allowedRoles: []string{"admin", "user"},
			wantStatus:   http.StatusUnauthorized,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			app := fiber.New()
			if tc.identity != nil {
				app.Get("/", withIdentity(tc.identity), RequireAnyRole(tc.allowedRoles...), ok)
			} else {
				app.Get("/", RequireAnyRole(tc.allowedRoles...), ok)
			}
			resp := get(t, app, "/")
			assert.Equal(t, tc.wantStatus, resp.StatusCode)
			if tc.wantStatus == http.StatusForbidden {
				body := bodyStr(t, resp)
				for _, role := range tc.allowedRoles {
					assert.Contains(t, body, role)
				}
			}
		})
	}
}

func TestStacking_AuthThenRBAC(t *testing.T) {
	tests := []struct {
		name       string
		identity   *auth.Identity
		wantStatus int
	}{
		{
			name:       "admin passes RequireAdmin",
			identity:   &auth.Identity{Subject: "admin-1", Roles: []string{"admin"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "user blocked by RequireAdmin",
			identity:   &auth.Identity{Subject: "user-1", Roles: []string{"user"}},
			wantStatus: http.StatusForbidden,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			app := fiber.New()
			// withIdentity simulates auth.New; RequireAdmin is the guard under test.
			app.Get("/admin", withIdentity(tc.identity), RequireAdmin(), ok)
			assert.Equal(t, tc.wantStatus, get(t, app, "/admin").StatusCode)
		})
	}
}
