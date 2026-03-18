package auth

import (
	"slices"

	"github.com/gofiber/fiber/v3"
)

// contextKey is an unexported type for context keys defined in this package.
// Using a typed key prevents collisions with keys defined in other packages.
type contextKey int

const identityKey contextKey = iota

// Identity holds the validated, trusted user information extracted from
// a Keycloak access token.
type Identity struct {
	// Subject is the Keycloak user UUID (the "sub" JWT claim).
	Subject string

	// Email is the email claim, falling back to preferred_username if absent.
	Email string

	// Roles is the filtered list of realm-level roles assigned to this user.
	// Keycloak system roles are stripped.
	Roles []string
}

// HasRole returns true if this identity carries the given role.
func (id *Identity) HasRole(role string) bool {
	return slices.Contains(id.Roles, role)
}

// IsAdmin is a convenience wrapper for HasRole("admin").
func (id *Identity) IsAdmin() bool {
	return id.HasRole("admin")
}

// IsUser is a convenience wrapper for HasRole("user").
func (id *Identity) IsUser() bool {
	return id.HasRole("user")
}

// SetIdentity stores the identity in the request context.
// Called internally by the middleware after successful JWT validation or header trust.
func SetIdentity(c fiber.Ctx, id *Identity) {
	fiber.StoreInContext(c, identityKey, id)
}

// GetIdentity retrieves the identity from the request context.
// Returns nil if the route is not protected by the auth middleware.
func GetIdentity(c fiber.Ctx) *Identity {
	id, _ := fiber.ValueFromContext[*Identity](c, identityKey)
	return id
}

// IdentityFromContext returns the Identity found in the context.
// It accepts fiber.Ctx, *fasthttp.RequestCtx, and context.Context.
// It returns nil if the identity does not exist.
func IdentityFromContext(ctx any) *Identity {
	id, _ := fiber.ValueFromContext[*Identity](ctx, identityKey)
	return id
}

// MustGetIdentity retrieves the identity and panics if absent.
// Use this inside handlers that are guaranteed to sit behind the middleware;
// a panic here is always a wiring mistake, not a user error.
func MustGetIdentity(c fiber.Ctx) *Identity {
	id := GetIdentity(c)
	if id == nil {
		panic("authmw: identity not found in context; either this route is not protected by the auth middleware or there has been an internal error")
	}
	return id
}
