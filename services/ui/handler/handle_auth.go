package handler

import (
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v3"
	"go.uber.org/zap"

	"github.com/mirstar13/go-map-reduce/services/ui/client"
)

// AuthHandler handles authentication-related routes.
type AuthHandler struct {
	kc  *client.KeycloakClient
	log *zap.Logger
}

// NewAuthHandler creates a new AuthHandler.
func NewAuthHandler(kc *client.KeycloakClient, log *zap.Logger) *AuthHandler {
	return &AuthHandler{kc: kc, log: log}
}

// loginRequest is the expected JSON body for POST /auth/login.
type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// Login
//
//	POST /auth/login
//	Public; no JWT required.
//
// Proxies the Keycloak direct-grant flow and returns the access token.
// The CLI stores the token and includes it as "Authorization: Bearer <token>"
// on all subsequent requests.
func (h *AuthHandler) Login(c fiber.Ctx) error {
	var req loginRequest
	if err := c.Bind().JSON(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid request body",
		})
	}

	if req.Username == "" || req.Password == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "username and password are required",
		})
	}

	tr, err := h.kc.Login(c.Context(), req.Username, req.Password)
	if err != nil {
		h.log.Warn("login failed", zap.String("username", req.Username), zap.Error(err))
		
		// Map specific error messages to help the user debug.
		msg := "invalid credentials"
		if err != nil && (strings.Contains(err.Error(), "connection") || strings.Contains(err.Error(), "refused") || strings.Contains(err.Error(), "lookup")) {
			msg = "auth service unreachable"
		} else if err != nil && strings.Contains(err.Error(), "status") {
			// Extract the status and message from the Keycloak error if possible
			msg = fmt.Sprintf("auth failed: %v", err)
		}

		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": msg,
		})
	}

	h.log.Info("user logged in", zap.String("username", req.Username))

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"access_token":       tr.AccessToken,
		"expires_in":         tr.ExpiresIn,
		"refresh_token":      tr.RefreshToken,
		"refresh_expires_in": tr.RefreshExpiresIn,
		"token_type":         tr.TokenType,
	})
}
