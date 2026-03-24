package auth

import (
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/mirstar13/go-map-reduce/pkg/jwks"
)

// New creates a new middleware handler that validates Keycloak-issued JWTs.
func New(config ...Config) fiber.Handler {
	cfg := configDefault(config...)

	cache := jwks.New(cfg.jwksURL(), cfg.JWKSRefreshTTL)
	expectedIssuer := cfg.issuer()

	return func(c fiber.Ctx) error {
		// Skip middleware if Next returns true.
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}

		rawToken, err := extractBearer(c)
		if err != nil {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": err.Error(),
			})
		}

		claims, err := parseAndVerify(rawToken, cache, expectedIssuer, cfg.ClientID)
		if err != nil {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": err.Error(),
			})
		}

		id := claimsToIdentity(claims)
		SetIdentity(c, id)

		// Forward identity as trusted headers so internal services can read
		// the caller's identity without re-validating the token.
		c.Request().Header.Set("X-User-Sub", id.Subject)
		c.Request().Header.Set("X-User-Email", id.Email)
		c.Request().Header.Set("X-User-Roles", strings.Join(id.Roles, ","))

		return c.Next()
	}
}

// NewInternal returns a lightweight middleware for internal services that trusts
// the X-User-* headers forwarded by the UI service after full JWT validation.
//
// IMPORTANT: Only safe because internal services are ClusterIP; not reachable
// from the public internet. Never use this on a service exposed via NodePort or Ingress.
func NewInternal() fiber.Handler {
	return func(c fiber.Ctx) error {
		sub := c.Get("X-User-Sub")
		if sub == "" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "missing X-User-Sub header; request did not pass through the UI gateway",
			})
		}

		var roles []string
		for _, r := range strings.Split(c.Get("X-User-Roles"), ",") {
			r = strings.TrimSpace(r)
			if r != "" {
				roles = append(roles, r)
			}
		}

		SetIdentity(c, &Identity{
			Subject: sub,
			Email:   c.Get("X-User-Email"),
			Roles:   roles,
		})

		return c.Next()
	}
}

// extractBearer pulls the raw JWT string from the Authorization: Bearer <token> header.
func extractBearer(c fiber.Ctx) (string, error) {
	authHeader := c.Get("Authorization")
	if authHeader == "" {
		return "", fmt.Errorf("missing authorization header")
	}

	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", fmt.Errorf("authorization header must be of the form 'Bearer <token>'")
	}

	token := strings.TrimSpace(parts[1])
	if token == "" {
		return "", fmt.Errorf("empty Bearer token")
	}

	return token, nil
}

// parseAndVerify parses the JWT, selects the correct RSA public key by kid,
// and validates signature, expiry, issuer, and authorised party.
func parseAndVerify(rawToken string, cache *jwks.Cache, expectedIssuer string, expectedClientID string) (*keycloakClaims, error) {
	claims := &keycloakClaims{}

	token, err := jwt.ParseWithClaims(rawToken, claims, func(t *jwt.Token) (any, error) {
		// Keycloak always uses RS256. Reject anything else to prevent alg-confusion attacks.
		if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}

		kid, ok := t.Header["kid"].(string)
		if !ok || kid == "" {
			return nil, fmt.Errorf("missing kid in JWT header")
		}

		return cache.PublicKey(kid)
	}, jwt.WithExpirationRequired(), jwt.WithIssuedAt())

	if err != nil {
		return nil, fmt.Errorf("invalid token: %w", err)
	}

	if !token.Valid {
		return nil, fmt.Errorf("token validation failed")
	}

	// Issuer check — ensures this token was issued by our Keycloak realm.
	iss, err := claims.GetIssuer()
	if err != nil || iss != expectedIssuer {
		return nil, fmt.Errorf("token issuer %q does not match expected %q", iss, expectedIssuer)
	}

	// Authorised-party check — prevents token reuse across OIDC clients in the same realm.
	if expectedClientID != "" && claims.AuthorizedParty != expectedClientID {
		return nil, fmt.Errorf("token azp %q does not match client %q", claims.AuthorizedParty, expectedClientID)
	}

	// Defensive: the sub claim must be present. An empty Subject means the token
	// is malformed or the JWT parser failed to extract the claim — either way we
	// must not allow the request through, because every downstream service relies
	// on X-User-Sub being non-empty to identify the caller.
	if claims.Subject == "" {
		return nil, fmt.Errorf("token is missing the 'sub' (subject) claim")
	}

	return claims, nil
}

// claimsToIdentity converts parsed Keycloak JWT claims into our Identity type,
// normalising the email field and filtering Keycloak system roles.
func claimsToIdentity(c *keycloakClaims) *Identity {
	email := c.Email
	if email == "" {
		email = c.PreferredUsername
	}

	return &Identity{
		Subject: c.Subject, // directly from the top-level "sub" field — never empty
		Email:   email,
		Roles:   filterRoles(c.RealmAccess.Roles),
	}
}
