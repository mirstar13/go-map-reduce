package auth

import (
	"fmt"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
)

// Config defines the config for the Authentication middleware.
type Config struct {
	// Next defines a function to skip this middleware when returned true.
	//
	// Optional. Default: nil
	Next func(c fiber.Ctx) bool

	// KeycloakURL is the base URL of the Keycloak instance.
	//
	// Required.
	KeycloakURL string

	// Realm is the Keycloak realm name.
	//
	// Optional. Default: "mapreduce"
	Realm string

	// ClientID is the OIDC client configured in Keycloak.
	// Used only to validate the "azp" (authorised party) claim.
	// Ensures tokens issued for a different client are rejected.
	//
	// Required.
	ClientID string

	// JWKSRefreshTTL controls how often the JWKS key cache is considered stale.
	//
	// Optional. Default: 5 minutes
	JWKSRefreshTTL time.Duration
}

// ConfigDefault is the default config.
var ConfigDefault = Config{
	Next:           nil,
	KeycloakURL:    "",
	Realm:          "mapreduce",
	ClientID:       "",
	JWKSRefreshTTL: 5 * time.Minute,
}

// configDefault sets default values for fields the caller left at their zero value.
func configDefault(config ...Config) Config {
	cfg := ConfigDefault

	if len(config) > 0 {
		cfg = config[0]

		if cfg.Realm == "" {
			cfg.Realm = ConfigDefault.Realm
		}

		if cfg.JWKSRefreshTTL <= 0 {
			cfg.JWKSRefreshTTL = ConfigDefault.JWKSRefreshTTL
		}
	}

	if cfg.KeycloakURL == "" {
		panic("auth: KeycloakURL is required")
	}

	if cfg.ClientID == "" {
		panic("auth: ClientID is required")
	}

	return cfg
}

func (c *Config) jwksURL() string {
	return fmt.Sprintf("%s/realms/%s/protocol/openid-connect/certs", strings.TrimRight(c.KeycloakURL, "/"), c.Realm)
}

func (c *Config) issuer() string {
	return fmt.Sprintf("%s/realms/%s", strings.TrimRight(c.KeycloakURL, "/"), c.Realm)
}
