package auth

import "github.com/golang-jwt/jwt/v5"

// keycloakClaims mirrors the relevant fields Keycloack puts into its access tokens.
// Fields beyond the registered JWT claims (sub, exp, iss, etc.) are Keycloack-specific.
type keycloakClaims struct {
	jwt.RegisteredClaims

	// Standard OIDC fields
	Email             string `json:"email"`
	PreferredUsername string `json:"preferred_username"`
	AuthorizedParty   string `json:"azp"`

	// Keycloak realm-level roles live under realm_access.roles.
	RealmAccess struct {
		Roles []string `json:"roles"`
	} `json:"realm_access"`
}

// keycloakSystemRoles are internal Keycloack roles.
//
// WARNING: These roles should never be forwarded to
// application logic as meaningful roles.
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
