package jwks

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func generateKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err, "generate RSA key")
	return key
}

func jwksServerFor(t *testing.T, kid string, pub *rsa.PublicKey) *httptest.Server {
	t.Helper()
	payload := buildJWKS(kid, pub)
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(payload)
	}))
}

func buildJWKS(kid string, pub *rsa.PublicKey) jwksResponse {
	return jwksResponse{
		Keys: []jwksKey{
			{
				Kid: kid,
				Kty: "RSA",
				Use: "sig",
				N:   base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
				E:   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes()),
			},
		},
	}
}

func TestParseRSAPublicKey_RoundTrip(t *testing.T) {
	priv := generateKey(t)
	pub := &priv.PublicKey

	nB64 := base64.RawURLEncoding.EncodeToString(pub.N.Bytes())
	eB64 := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes())

	got, err := parseRSAPublicKey(nB64, eB64)
	require.NoError(t, err)

	assert.Equal(t, pub.N, got.N, "modulus must round-trip")
	assert.Equal(t, pub.E, got.E, "exponent must round-trip")
}

func TestParseRSAPublicKey_BadModulus(t *testing.T) {
	_, err := parseRSAPublicKey("not-valid-base64!!!", "AQAB")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "decode modulus")
}

func TestParseRSAPublicKey_BadExponent(t *testing.T) {
	priv := generateKey(t)
	nB64 := base64.RawURLEncoding.EncodeToString(priv.PublicKey.N.Bytes())
	_, err := parseRSAPublicKey(nB64, "not-valid-base64!!!")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "decode exponent")
}

func TestParseRSAPublicKey_ZeroExponent(t *testing.T) {
	priv := generateKey(t)
	nB64 := base64.RawURLEncoding.EncodeToString(priv.PublicKey.N.Bytes())
	// base64url of a single 0x00 byte decodes to the integer 0
	eB64 := base64.RawURLEncoding.EncodeToString([]byte{0x00})
	_, err := parseRSAPublicKey(nB64, eB64)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "zero")
}

func TestCache_PublicKey_HappyPath(t *testing.T) {
	priv := generateKey(t)
	srv := jwksServerFor(t, "key-1", &priv.PublicKey)
	defer srv.Close()

	cache := New(srv.URL, time.Minute)
	got, err := cache.PublicKey("key-1")

	require.NoError(t, err)
	assert.Equal(t, priv.PublicKey.N, got.N)
	assert.Equal(t, priv.PublicKey.E, got.E)
}

func TestCache_PublicKey_CacheHit_NoExtraFetch(t *testing.T) {
	priv := generateKey(t)
	fetchCount := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetchCount++
		payload := buildJWKS("key-1", &priv.PublicKey)
		json.NewEncoder(w).Encode(payload)
	}))
	defer srv.Close()

	cache := New(srv.URL, time.Minute)

	// First call triggers a fetch.
	_, err := cache.PublicKey("key-1")
	require.NoError(t, err)

	// Second call within TTL must not trigger another fetch.
	_, err = cache.PublicKey("key-1")
	require.NoError(t, err)

	assert.Equal(t, 1, fetchCount, "expected exactly one HTTP fetch within TTL")
}

func TestCache_PublicKey_UnknownKid(t *testing.T) {
	priv := generateKey(t)
	srv := jwksServerFor(t, "key-1", &priv.PublicKey)
	defer srv.Close()

	cache := New(srv.URL, time.Minute)
	_, err := cache.PublicKey("key-does-not-exist")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown signing key id")
}

func TestCache_PublicKey_StaleRefresh(t *testing.T) {
	priv := generateKey(t)
	fetchCount := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetchCount++
		payload := buildJWKS("key-1", &priv.PublicKey)
		json.NewEncoder(w).Encode(payload)
	}))
	defer srv.Close()

	// Use a 1 ns TTL so the cache is stale on the second call.
	cache := New(srv.URL, time.Nanosecond)

	_, err := cache.PublicKey("key-1")
	require.NoError(t, err)

	time.Sleep(time.Millisecond) // ensure TTL has elapsed

	_, err = cache.PublicKey("key-1")
	require.NoError(t, err)

	assert.Equal(t, 2, fetchCount, "expected a second fetch after TTL expiry")
}

func TestCache_PublicKey_ServerDown_StaleKeyFallback(t *testing.T) {
	priv := generateKey(t)
	srv := jwksServerFor(t, "key-1", &priv.PublicKey)

	// Populate the cache while the server is up.
	cache := New(srv.URL, time.Nanosecond)
	_, err := cache.PublicKey("key-1")
	require.NoError(t, err)

	// Tear down the server, let TTL expire.
	srv.Close()
	time.Sleep(time.Millisecond)

	// Must return the stale key rather than an error.
	got, err := cache.PublicKey("key-1")
	require.NoError(t, err, "should fall back to stale key when server is down")
	assert.Equal(t, priv.PublicKey.N, got.N)
}

func TestCache_PublicKey_ServerNeverUp(t *testing.T) {
	// Use a port that nothing is listening on.
	cache := New("http://127.0.0.1:19999", time.Minute)
	_, err := cache.PublicKey("any-kid")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refresh failed")
}

func TestCache_Refresh_SkipsEncryptionKeys(t *testing.T) {
	priv := generateKey(t)

	// Serve two keys: one "enc" (must be skipped) and one "sig" (must load).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload := jwksResponse{Keys: []jwksKey{
			{
				Kid: "enc-key",
				Kty: "RSA",
				Use: "enc", // encryption key — must be ignored
				N:   base64.RawURLEncoding.EncodeToString(priv.PublicKey.N.Bytes()),
				E:   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(priv.PublicKey.E)).Bytes()),
			},
			{
				Kid: "sig-key",
				Kty: "RSA",
				Use: "sig",
				N:   base64.RawURLEncoding.EncodeToString(priv.PublicKey.N.Bytes()),
				E:   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(priv.PublicKey.E)).Bytes()),
			},
		}}
		json.NewEncoder(w).Encode(payload)
	}))
	defer srv.Close()

	cache := New(srv.URL, time.Minute)

	_, err := cache.PublicKey("sig-key")
	require.NoError(t, err, "sig key should load fine")

	_, err = cache.PublicKey("enc-key")
	require.Error(t, err, "enc key should not be in the cache")
}

func TestCache_Refresh_EmptyJWKS(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return a JWKS with no usable signing keys.
		json.NewEncoder(w).Encode(jwksResponse{Keys: []jwksKey{}})
	}))
	defer srv.Close()

	cache := New(srv.URL, time.Minute)
	_, err := cache.PublicKey("any")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no RSA signing keys found")
}
