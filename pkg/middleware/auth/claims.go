package auth

import "github.com/golang-jwt/jwt/v5"

// keycloakClaims mirrors the relevant fields Keycloak puts into its access tokens.
//
// IMPORTANT: We deliberately do NOT embed jwt.RegisteredClaims here. Struct
// embedding causes JSON-unmarshaling ambiguity in golang-jwt/v5: real Keycloak
// tokens include "aud" as a JSON array (["account"]), whose custom
// ClaimStrings.UnmarshalJSON interacts badly with the embedded-struct field
// promotion, leaving RegisteredClaims.Subject (the "sub" claim) silently empty
// even though the token is otherwise valid. By defining every field explicitly
// at the top level we get unambiguous JSON decoding and a reliable Subject.
type keycloakClaims struct {
	// Standard JWT registered claims (RFC 7519) — all defined explicitly.
	Issuer    string           `json:"iss,omitempty"`
	Subject   string           `json:"sub,omitempty"` // Keycloak user UUID
	Audience  jwt.ClaimStrings `json:"aud,omitempty"`
	ExpiresAt *jwt.NumericDate `json:"exp,omitempty"`
	NotBefore *jwt.NumericDate `json:"nbf,omitempty"`
	IssuedAt  *jwt.NumericDate `json:"iat,omitempty"`
	JWTID     string           `json:"jti,omitempty"`

	// Standard OIDC fields
	Email             string `json:"email"`
	PreferredUsername string `json:"preferred_username"`
	AuthorizedParty   string `json:"azp"`

	// Keycloak realm-level roles live under realm_access.roles.
	RealmAccess struct {
		Roles []string `json:"roles"`
	} `json:"realm_access"`
}


func (c keycloakClaims) GetExpirationTime() (*jwt.NumericDate, error) {
	return c.ExpiresAt, nil
}

func (c keycloakClaims) GetIssuedAt() (*jwt.NumericDate, error) {
	return c.IssuedAt, nil
}

func (c keycloakClaims) GetNotBefore() (*jwt.NumericDate, error) {
	return c.NotBefore, nil
}

func (c keycloakClaims) GetIssuer() (string, error) {
	return c.Issuer, nil
}

func (c keycloakClaims) GetSubject() (string, error) {
	return c.Subject, nil
}

func (c keycloakClaims) GetAudience() (jwt.ClaimStrings, error) {
	return c.Audience, nil
}

// keycloakSystemRoles are internal Keycloak roles.
//
// WARNING: These roles should never be forwarded to application logic as
// meaningful roles.
var keycloakSystemRoles = map[string]struct{}{
	"offline_access":          {},
	"uma_authorization":       {},
	"default-roles-mapreduce": {},
}

func filterRoles(raw []string) []string {
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		if _, skip := keycloakSystemRoles[r]; !skip {
			out = append(out, r)
		}
	}
	return out
}
