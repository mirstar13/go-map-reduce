package handler

import (
	"github.com/gofiber/fiber/v3"
	"go.uber.org/zap"

	"github.com/mirstar13/go-map-reduce/pkg/middleware/auth"
	"github.com/mirstar13/go-map-reduce/services/ui/client"
)

// AdminHandler handles admin-only routes.
// Requires the caller to hold the "admin" realm role (enforced by RBAC middleware
// registered in main.go — not here, to keep handlers free of routing concerns).
type AdminHandler struct {
	manager  *client.ManagerClient
	keycloak *client.KeycloakClient
	log      *zap.Logger
}

// NewAdminHandler creates a new AdminHandler.
func NewAdminHandler(
	manager *client.ManagerClient,
	kc *client.KeycloakClient,
	log *zap.Logger,
) *AdminHandler {
	return &AdminHandler{manager: manager, keycloak: kc, log: log}
}

// ListAllJobs godoc
//
//	GET /admin/jobs
//	Requires: admin role.
//
// Returns all jobs in the system regardless of owner.
func (h *AdminHandler) ListAllJobs(c fiber.Ctx) error {
	id := auth.GetIdentity(c)
	raw, status, err := h.manager.AdminListJobs(c.Context(), id.Subject, id.Email, rolesHeader(id))
	if err != nil {
		h.log.Error("admin list jobs: manager error", zap.Error(err))
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": "upstream error"})
	}
	return c.Status(status).Send(raw)
}

// ListUsers godoc
//
//	GET /admin/users
//	Requires: admin role.
//
// Returns all users in the configured Keycloak realm.
func (h *AdminHandler) ListUsers(c fiber.Ctx) error {
	users, err := h.keycloak.ListUsers(c.Context())
	if err != nil {
		h.log.Error("admin list users", zap.Error(err))
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": "could not list users"})
	}
	return c.Status(fiber.StatusOK).JSON(users)
}

// GetUser godoc
//
//	GET /admin/users/:id
//	Requires: admin role.
//
// Returns a single user by their Keycloak ID.
func (h *AdminHandler) GetUser(c fiber.Ctx) error {
	userID := c.Params("id")
	if userID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "user id is required"})
	}

	user, err := h.keycloak.GetUser(c.Context(), userID)
	if err != nil {
		h.log.Error("admin get user", zap.String("user_id", userID), zap.Error(err))
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": "could not get user"})
	}
	return c.Status(fiber.StatusOK).JSON(user)
}

// createUserRequest is the body for POST /admin/users.
type createUserRequest struct {
	Username string `json:"username"`
	Email    string `json:"email"`
	Password string `json:"password"`
	// Role is the realm role to assign: "user" or "admin".
	// Defaults to "user" if omitted.
	Role string `json:"role"`
}

// CreateUser godoc
//
//	POST /admin/users
//	Requires: admin role.
//
// Creates a new user in Keycloak and assigns the specified realm role.
//
// Example body:
//
//	{"username": "bob", "email": "bob@example.com", "password": "s3cr3t", "role": "user"}
func (h *AdminHandler) CreateUser(c fiber.Ctx) error {
	var req createUserRequest
	if err := c.Bind().JSON(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	if req.Username == "" || req.Email == "" || req.Password == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "username, email and password are required",
		})
	}

	role := req.Role
	if role == "" {
		role = "user"
	}
	if role != "user" && role != "admin" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "role must be 'user' or 'admin'",
		})
	}

	kcReq := client.CreateUserRequest{
		Username: req.Username,
		Email:    req.Email,
		Enabled:  true,
		Credentials: []struct {
			Type      string `json:"type"`
			Value     string `json:"value"`
			Temporary bool   `json:"temporary"`
		}{
			{Type: "password", Value: req.Password, Temporary: false},
		},
	}

	newID, err := h.keycloak.CreateUser(c.Context(), kcReq, role)
	if err != nil {
		h.log.Error("admin create user",
			zap.String("username", req.Username),
			zap.Error(err),
		)
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": "could not create user"})
	}

	h.log.Info("user created",
		zap.String("username", req.Username),
		zap.String("keycloak_id", newID),
		zap.String("role", role),
	)

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"id":       newID,
		"username": req.Username,
		"email":    req.Email,
		"role":     role,
	})
}

// DeleteUser godoc
//
//	DELETE /admin/users/:id
//	Requires: admin role.
//
// Permanently removes a user from Keycloak.
func (h *AdminHandler) DeleteUser(c fiber.Ctx) error {
	userID := c.Params("id")
	if userID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "user id is required"})
	}

	if err := h.keycloak.DeleteUser(c.Context(), userID); err != nil {
		h.log.Error("admin delete user", zap.String("user_id", userID), zap.Error(err))
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": "could not delete user"})
	}

	h.log.Info("user deleted", zap.String("user_id", userID))
	return c.Status(fiber.StatusNoContent).Send(nil)
}

// AssignRole godoc
//
//	POST /admin/users/:id/roles
//	Requires: admin role.
//
// Assigns a realm role to an existing user.
//
// Example body: {"role": "admin"}
func (h *AdminHandler) AssignRole(c fiber.Ctx) error {
	userID := c.Params("id")
	if userID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "user id is required"})
	}

	var body struct {
		Role string `json:"role"`
	}
	if err := c.Bind().JSON(&body); err != nil || body.Role == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "role is required"})
	}

	if body.Role != "user" && body.Role != "admin" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "role must be 'user' or 'admin'",
		})
	}

	if err := h.keycloak.AssignRole(c.Context(), userID, body.Role); err != nil {
		h.log.Error("admin assign role",
			zap.String("user_id", userID),
			zap.String("role", body.Role),
			zap.Error(err),
		)
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": "could not assign role"})
	}

	h.log.Info("role assigned",
		zap.String("user_id", userID),
		zap.String("role", body.Role),
	)
	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"user_id": userID,
		"role":    body.Role,
	})
}
