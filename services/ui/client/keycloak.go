package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/mirstar13/go-map-reduce/services/ui/config"
)

// KeycloakClient wraps the Keycloak Admin REST API.
// It caches the admin access token and refreshes it automatically when it expires.
type KeycloakClient struct {
	cfg        *config.Config
	httpClient *http.Client

	mu          sync.Mutex
	adminToken  string
	tokenExpiry time.Time
}

// NewKeycloakClient creates a ready-to-use KeycloakClient.
func NewKeycloakClient(cfg *config.Config) *KeycloakClient {
	return &KeycloakClient{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: 15 * time.Second},
	}
}

// TokenResponse is the response body from Keycloak's token endpoint.
type TokenResponse struct {
	AccessToken      string `json:"access_token"`
	ExpiresIn        int    `json:"expires_in"`
	RefreshToken     string `json:"refresh_token"`
	RefreshExpiresIn int    `json:"refresh_expires_in"`
	TokenType        string `json:"token_type"`
}

// Login exchanges username / password for a JWT via the direct-grant flow.
func (k *KeycloakClient) Login(ctx context.Context, username, password string) (*TokenResponse, error) {
	endpoint := fmt.Sprintf("%s/realms/%s/protocol/openid-connect/token",
		k.cfg.KeycloakURL, k.cfg.KeycloakRealm)

	form := url.Values{
		"grant_type": {"password"},
		"client_id":  {k.cfg.KeycloakClientID},
		"username":   {username},
		"password":   {password},
		"scope":      {"openid profile email roles"},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("keycloak: build login request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := k.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("keycloak: login request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("keycloak: login failed (status %d): %s", resp.StatusCode, body)
	}

	var tr TokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return nil, fmt.Errorf("keycloak: decode token response: %w", err)
	}
	return &tr, nil
}

// KCUser represents a Keycloak user as returned by the Admin REST API.
type KCUser struct {
	ID         string              `json:"id"`
	Username   string              `json:"username"`
	Email      string              `json:"email"`
	Enabled    bool                `json:"enabled"`
	RealmRoles []string            `json:"realmRoles,omitempty"`
	Attributes map[string][]string `json:"attributes,omitempty"`
}

// CreateUserRequest is the payload for creating a new Keycloak user.
type CreateUserRequest struct {
	Username    string `json:"username"`
	Email       string `json:"email"`
	Enabled     bool   `json:"enabled"`
	Credentials []struct {
		Type      string `json:"type"`
		Value     string `json:"value"`
		Temporary bool   `json:"temporary"`
	} `json:"credentials,omitempty"`
}

// ListUsers returns all users in the configured realm.
func (k *KeycloakClient) ListUsers(ctx context.Context) ([]KCUser, error) {
	raw, err := k.adminGet(ctx, "/users?max=200")
	if err != nil {
		return nil, err
	}
	var users []KCUser
	if err := json.Unmarshal(raw, &users); err != nil {
		return nil, fmt.Errorf("keycloak: decode users: %w", err)
	}
	return users, nil
}

// GetUser fetches a single user by Keycloak ID.
func (k *KeycloakClient) GetUser(ctx context.Context, userID string) (*KCUser, error) {
	raw, err := k.adminGet(ctx, "/users/"+userID)
	if err != nil {
		return nil, err
	}
	var user KCUser
	if err := json.Unmarshal(raw, &user); err != nil {
		return nil, fmt.Errorf("keycloak: decode user: %w", err)
	}
	return &user, nil
}

// CreateUser creates a new user and optionally assigns a realm role.
// Returns the new user's Keycloak ID.
func (k *KeycloakClient) CreateUser(ctx context.Context, req CreateUserRequest, role string) (string, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("keycloak: marshal create user: %w", err)
	}

	// POST /users returns 201 with Location header containing the new user's URL.
	location, err := k.adminPost(ctx, "/users", body)
	if err != nil {
		return "", err
	}

	// Extract user ID from the Location header (last path segment).
	parts := strings.Split(strings.TrimRight(location, "/"), "/")
	newUserID := parts[len(parts)-1]

	if role != "" {
		if err := k.AssignRole(ctx, newUserID, role); err != nil {
			return newUserID, fmt.Errorf("keycloak: assign role %q: %w", role, err)
		}
	}

	return newUserID, nil
}

// DeleteUser removes a user from the realm by Keycloak ID.
func (k *KeycloakClient) DeleteUser(ctx context.Context, userID string) error {
	return k.adminDelete(ctx, "/users/"+userID)
}

// AssignRole adds a realm role to a user.
func (k *KeycloakClient) AssignRole(ctx context.Context, userID, roleName string) error {
	// First resolve the role representation.
	roleRaw, err := k.adminGet(ctx, "/roles/"+roleName)
	if err != nil {
		return fmt.Errorf("keycloak: get role %q: %w", roleName, err)
	}

	// POST to /users/{id}/role-mappings/realm expects an array of role representations.
	payload := fmt.Sprintf("[%s]", roleRaw)
	_, err = k.adminPost(ctx, "/users/"+userID+"/role-mappings/realm", []byte(payload))
	return err
}

// adminGet performs a GET against the Keycloak Admin REST API path relative to
// /admin/realms/{realm} and returns the raw response body.
func (k *KeycloakClient) adminGet(ctx context.Context, path string) ([]byte, error) {
	return k.adminDo(ctx, http.MethodGet, path, nil)
}

// adminPost performs a POST and returns the Location header value (for creates).
func (k *KeycloakClient) adminPost(ctx context.Context, path string, body []byte) (string, error) {
	token, err := k.getAdminToken(ctx)
	if err != nil {
		return "", err
	}

	url := k.adminBase() + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("keycloak admin: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := k.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("keycloak admin: POST %s: %w", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("keycloak admin: POST %s status %d: %s", path, resp.StatusCode, b)
	}

	return resp.Header.Get("Location"), nil
}

// adminDelete performs a DELETE against the Keycloak Admin REST API.
func (k *KeycloakClient) adminDelete(ctx context.Context, path string) error {
	_, err := k.adminDo(ctx, http.MethodDelete, path, nil)
	return err
}

func (k *KeycloakClient) adminDo(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	token, err := k.getAdminToken(ctx)
	if err != nil {
		return nil, err
	}

	var bodyReader io.Reader
	if len(body) > 0 {
		bodyReader = bytes.NewReader(body)
	}

	url := k.adminBase() + path
	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("keycloak admin: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := k.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("keycloak admin: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("keycloak admin: read body: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("keycloak admin: %s %s status %d: %s", method, path, resp.StatusCode, raw)
	}

	return raw, nil
}

// adminBase returns the base URL for the Keycloak Admin REST API.
func (k *KeycloakClient) adminBase() string {
	return fmt.Sprintf("%s/admin/realms/%s", k.cfg.KeycloakURL, k.cfg.KeycloakRealm)
}

// getAdminToken returns a valid admin token, refreshing if expired.
func (k *KeycloakClient) getAdminToken(ctx context.Context) (string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	// Return cached token if still valid (with 30-second buffer).
	if k.adminToken != "" && time.Now().Add(30*time.Second).Before(k.tokenExpiry) {
		return k.adminToken, nil
	}

	// Fetch a fresh admin token from the master realm.
	endpoint := fmt.Sprintf("%s/realms/master/protocol/openid-connect/token", k.cfg.KeycloakURL)
	form := url.Values{
		"grant_type": {"password"},
		"client_id":  {"admin-cli"},
		"username":   {k.cfg.KeycloakAdminUser},
		"password":   {k.cfg.KeycloakAdminPassword},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("keycloak: build admin token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := k.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("keycloak: admin token request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("keycloak: admin token failed (status %d): %s", resp.StatusCode, b)
	}

	var tr struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return "", fmt.Errorf("keycloak: decode admin token: %w", err)
	}

	k.adminToken = tr.AccessToken
	k.tokenExpiry = time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second)
	return k.adminToken, nil
}
