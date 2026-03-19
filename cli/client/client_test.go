package client

import (
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// jsonServer returns a test server that always responds with the given status
// code and JSON body.
func jsonServer(t *testing.T, status int, body interface{}) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(body)
	}))
}

// captureServer returns a test server that records every incoming request and
// responds with the given status / body.
type capturedRequest struct {
	Method  string
	Path    string
	Headers http.Header
	Body    []byte
}

func captureServer(t *testing.T, status int, body interface{}) (*httptest.Server, *capturedRequest) {
	t.Helper()
	captured := &capturedRequest{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured.Method = r.Method
		captured.Path = r.URL.Path
		captured.Headers = r.Header.Clone()
		captured.Body, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(body)
	}))
	return srv, captured
}

func TestGet_Success_DecodesResponse(t *testing.T) {
	srv := jsonServer(t, http.StatusOK, map[string]string{"key": "value"})
	defer srv.Close()

	c := New(srv.URL, "mytoken")
	var result map[string]string
	require.NoError(t, c.Get("/test", &result))
	assert.Equal(t, "value", result["key"])
}

func TestGet_AttachesBearerToken(t *testing.T) {
	srv, captured := captureServer(t, http.StatusOK, map[string]string{})
	defer srv.Close()

	c := New(srv.URL, "super.secret.token")
	var result map[string]string
	require.NoError(t, c.Get("/path", &result))

	assert.Equal(t, "Bearer super.secret.token", captured.Headers.Get("Authorization"))
}

func TestGet_NoToken_NoAuthHeader(t *testing.T) {
	srv, captured := captureServer(t, http.StatusOK, map[string]string{})
	defer srv.Close()

	c := New(srv.URL, "") // empty token
	var result map[string]string
	require.NoError(t, c.Get("/path", &result))

	assert.Empty(t, captured.Headers.Get("Authorization"),
		"no auth header should be sent when token is empty")
}

func TestGet_4xx_ReturnsAPIError(t *testing.T) {
	srv := jsonServer(t, http.StatusForbidden, map[string]string{"error": "access denied"})
	defer srv.Close()

	c := New(srv.URL, "tok")
	var result map[string]string
	err := c.Get("/protected", &result)

	require.Error(t, err)
	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusForbidden, apiErr.Status)
	assert.Contains(t, apiErr.Message, "access denied")
}

func TestGet_5xx_ReturnsAPIError(t *testing.T) {
	srv := jsonServer(t, http.StatusInternalServerError, map[string]string{"error": "boom"})
	defer srv.Close()

	c := New(srv.URL, "tok")
	var result map[string]string
	err := c.Get("/resource", &result)

	require.Error(t, err)
	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusInternalServerError, apiErr.Status)
}

func TestGet_404_EmptyErrorField_UsesRawBody(t *testing.T) {
	// When the server returns a non-JSON body, the raw text becomes the message.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, "not found")
	}))
	defer srv.Close()

	c := New(srv.URL, "tok")
	var result interface{}
	err := c.Get("/missing", &result)

	require.Error(t, err)
	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusNotFound, apiErr.Status)
}

func TestPost_SendsJSONBody(t *testing.T) {
	srv, captured := captureServer(t, http.StatusCreated, map[string]string{"id": "abc"})
	defer srv.Close()

	c := New(srv.URL, "tok")
	payload := map[string]string{"name": "alice"}
	var result map[string]string
	require.NoError(t, c.Post("/resource", payload, &result))

	assert.Equal(t, "application/json", captured.Headers.Get("Content-Type"))

	var sent map[string]string
	require.NoError(t, json.Unmarshal(captured.Body, &sent))
	assert.Equal(t, "alice", sent["name"])
}

func TestPost_AttachesBearerToken(t *testing.T) {
	srv, captured := captureServer(t, http.StatusOK, map[string]string{})
	defer srv.Close()

	c := New(srv.URL, "post.token")
	require.NoError(t, c.Post("/res", map[string]string{}, nil))

	assert.Equal(t, "Bearer post.token", captured.Headers.Get("Authorization"))
}

func TestPost_4xx_ReturnsAPIError(t *testing.T) {
	srv := jsonServer(t, http.StatusBadRequest, map[string]string{"error": "bad input"})
	defer srv.Close()

	c := New(srv.URL, "tok")
	err := c.Post("/res", map[string]string{}, nil)

	require.Error(t, err)
	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	assert.Contains(t, apiErr.Message, "bad input")
}

func TestPost_NilBodyTarget_DoesNotPanic(t *testing.T) {
	// Passing nil as the result pointer must not panic even on a 2xx response.
	srv := jsonServer(t, http.StatusOK, map[string]string{"ok": "true"})
	defer srv.Close()

	c := New(srv.URL, "tok")
	assert.NotPanics(t, func() {
		_ = c.Post("/res", map[string]string{}, nil)
	})
}

func TestDelete_SendsDeleteMethod(t *testing.T) {
	srv, captured := captureServer(t, http.StatusOK, map[string]string{"status": "cancelled"})
	defer srv.Close()

	c := New(srv.URL, "tok")
	var result map[string]string
	require.NoError(t, c.Delete("/jobs/abc", &result))

	assert.Equal(t, http.MethodDelete, captured.Method)
	assert.Equal(t, "/jobs/abc", captured.Path)
	assert.Equal(t, "cancelled", result["status"])
}

func TestPostNoAuth_DoesNotSendAuthHeader(t *testing.T) {
	srv, captured := captureServer(t, http.StatusOK, map[string]string{"access_token": "newtoken"})
	defer srv.Close()

	c := New(srv.URL, "stored.token") // has a stored token
	var result map[string]string
	require.NoError(t, c.PostNoAuth("/auth/login", map[string]string{"u": "alice", "p": "secret"}, &result))

	assert.Empty(t, captured.Headers.Get("Authorization"),
		"PostNoAuth must not send the stored token")
	assert.Equal(t, "newtoken", result["access_token"])
}

func TestUploadFile_SendsMultipartFormData(t *testing.T) {
	// Verify the request is multipart and contains the correct file field.
	var capturedContentType string
	var capturedFileName string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedContentType = r.Header.Get("Content-Type")

		mt, _, _ := mime.ParseMediaType(capturedContentType)
		assert.Equal(t, "multipart/form-data", mt, "must be multipart/form-data")

		require.NoError(t, r.ParseMultipartForm(1<<20))
		fh := r.MultipartForm.File["file"]
		require.NotEmpty(t, fh)
		capturedFileName = fh[0].Filename

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"path": "input/data.jsonl"})
	}))
	defer srv.Close()

	// Create a temporary file to upload.
	tmp, err := os.CreateTemp(t.TempDir(), "data-*.jsonl")
	require.NoError(t, err)
	fmt.Fprintln(tmp, `{"id":1}`)
	tmp.Close()

	c := New(srv.URL, "tok")
	var result map[string]string
	require.NoError(t, c.UploadFile("/files/input", tmp.Name(), &result))

	assert.Equal(t, filepath.Base(tmp.Name()), capturedFileName)
	assert.Equal(t, "input/data.jsonl", result["path"])
}

func TestUploadFile_MissingLocalFile_ReturnsError(t *testing.T) {
	c := New("http://localhost:9999", "tok")
	err := c.UploadFile("/files/input", "/nonexistent/path.jsonl", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open")
}

func TestUploadFile_AttachesBearerToken(t *testing.T) {
	srv, captured := captureServer(t, http.StatusCreated, map[string]string{"path": "x"})
	defer srv.Close()

	tmp, err := os.CreateTemp(t.TempDir(), "*.jsonl")
	require.NoError(t, err)
	tmp.Close()

	c := New(srv.URL, "upload.token")
	var result map[string]string
	require.NoError(t, c.UploadFile("/files/input", tmp.Name(), &result))

	assert.Equal(t, "Bearer upload.token", captured.Headers.Get("Authorization"))
}

func TestAPIError_Error_FormatsCorrectly(t *testing.T) {
	err := &APIError{Status: 404, Message: "not found"}
	assert.Equal(t, "server error 404: not found", err.Error())
}

func TestPostRaw_Success_ReturnsBytesAndStatus(t *testing.T) {
	srv := jsonServer(t, http.StatusCreated, map[string]string{"job_id": "abc"})
	defer srv.Close()

	c := New(srv.URL, "tok")
	raw, status, err := c.PostRaw("/jobs", map[string]string{"mapper": "m.py"})

	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, status)

	var result map[string]string
	require.NoError(t, json.Unmarshal(raw, &result))
	assert.Equal(t, "abc", result["job_id"])
}

func TestPostRaw_4xx_ReturnsAPIError(t *testing.T) {
	srv := jsonServer(t, http.StatusUnprocessableEntity, map[string]string{"error": "validation failed"})
	defer srv.Close()

	c := New(srv.URL, "tok")
	_, _, err := c.PostRaw("/jobs", map[string]string{})

	require.Error(t, err)
	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)
	assert.Contains(t, strings.ToLower(apiErr.Message), "validation")
}
