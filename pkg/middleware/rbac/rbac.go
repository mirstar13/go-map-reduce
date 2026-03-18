package rbac

import (
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/mirstar13/go-map-reduce/pkg/middleware/auth"
)

// RequireRole returns a Fiber handler that aborts with 403 if the authenticated
// user does not hold the specified role. Must be used after auth.New() or auth.NewInternal().
func RequireRole(role string) fiber.Handler {
	return func(c fiber.Ctx) error {
		id := auth.GetIdentity(c)
		if id == nil {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "unauthenticated",
			})
		}

		if !id.HasRole(role) {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": "forbidden: requires role " + role,
			})
		}

		return c.Next()
	}
}

// RequireAdmin is a convenience alias for RequireRole("admin").
func RequireAdmin() fiber.Handler {
	return RequireRole("admin")
}

// RequireUser is a convenience alias for RequireRole("user").
func RequireUser() fiber.Handler {
	return RequireRole("user")
}

// RequireAnyRole returns a Fiber handler that passes if the authenticated user
// holds at least one of the given roles.
func RequireAnyRole(roles ...string) fiber.Handler {
	return func(c fiber.Ctx) error {
		id := auth.GetIdentity(c)
		if id == nil {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "unauthenticated",
			})
		}

		for _, role := range roles {
			if id.HasRole(role) {
				return c.Next()
			}
		}

		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": "forbidden: requires one of roles [" + strings.Join(roles, ", ") + "]",
		})
	}
}
