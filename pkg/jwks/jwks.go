package jwks

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"sync"
	"time"
)

// Cache fetches and caches Keycloak's RSA public signing keys.
type Cache struct {
	mu        sync.RWMutex
	keys      map[string]*rsa.PublicKey
	url       string
	fetchedAt time.Time
	ttl       time.Duration
	client    *http.Client
}

func New(url string, ttl time.Duration) *Cache {
	return &Cache{
		url:    url,
		ttl:    ttl,
		keys:   make(map[string]*rsa.PublicKey),
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *Cache) PublicKey(kid string) (*rsa.PublicKey, error) {
	c.mu.RLock()
	key, found := c.keys[kid]
	stale := time.Since(c.fetchedAt) > c.ttl
	c.mu.RUnlock()

	if found && !stale {
		return key, nil
	}

	if err := c.refresh(); err != nil {
		if found {
			return key, nil
		}
		return nil, fmt.Errorf("authmw: jwks refresh failed: %w", err)
	}

	c.mu.RLock()
	key, found = c.keys[kid]
	c.mu.RUnlock()

	if !found {
		return nil, fmt.Errorf("authmw: unknown signing key id %q; token may have been issued by a different realm or Keycloak instance", kid)
	}
	return key, nil
}

type jwksKey struct {
	Kid string `json:"kid"` // key ID — matches the JWT header "kid"
	Kty string `json:"kty"` // key type — we only handle "RSA"
	Alg string `json:"alg"` // algorithm — typically "RS256"
	Use string `json:"use"` // usage — we only use "sig" keys
	N   string `json:"n"`   // RSA modulus, base64url-encoded
	E   string `json:"e"`   // RSA public exponent, base64url-encoded
}

// Raw JSON shapes returned by Keycloak's JWKS endpoint.
type jwksResponse struct {
	Keys []jwksKey `json:"keys"`
}

func (c *Cache) refresh() error {
	resp, err := c.client.Get(c.url)
	if err != nil {
		return fmt.Errorf("GET %s: %w", c.url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s returned HTTP %d", c.url, resp.StatusCode)
	}

	var payload jwksResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return fmt.Errorf("decode jwks: %w", err)
	}

	next := make(map[string]*rsa.PublicKey, len(payload.Keys))
	for _, k := range payload.Keys {
		if k.Kty != "RSA" || k.Use != "sig" {
			continue // skip encryption keys and non-rsa entries
		}

		pub, err := parseRSAPublicKey(k.N, k.E)
		if err != nil {
			return fmt.Errorf("parse key %q: %w", k.Kid, err)
		}
		next[k.Kid] = pub
	}

	if len(next) == 0 {
		return fmt.Errorf("no RSA signing keys found in JWKS response from %s", c.url)
	}

	c.mu.Lock()
	c.keys = next
	c.fetchedAt = time.Now()
	c.mu.Unlock()
	return nil
}

func parseRSAPublicKey(nB64, eB64 string) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(nB64)
	if err != nil {
		return nil, fmt.Errorf("decode modulus: %w", err)
	}

	eBytes, err := base64.RawURLEncoding.DecodeString(eB64)
	if err != nil {
		return nil, fmt.Errorf("decode exponent: %w", err)
	}

	n := new(big.Int).SetBytes(nBytes)

	var e int
	for _, b := range eBytes {
		e = e<<8 | int(b)
	}

	if e == 0 {
		return nil, fmt.Errorf("exponent decoded to zero")
	}

	return &rsa.PublicKey{N: n, E: e}, nil
}
