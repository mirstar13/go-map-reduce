package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// Client is a thin wrapper around http.Client that handles auth and errors.
type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

// New creates a Client.
func New(baseURL, token string) *Client {
	return &Client{
		baseURL: baseURL,
		token:   token,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

// APIError is returned when the server responds with a non-2xx status.
type APIError struct {
	Status  int
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("server error %d: %s", e.Status, e.Message)
}

// Get performs an authenticated GET and decodes the JSON response into v.
func (c *Client) Get(path string, v any) error {
	req, err := http.NewRequest(http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	return c.do(req, v)
}

// Post performs an authenticated POST with a JSON body and decodes the response.
func (c *Client) Post(path string, body any, v any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}
	req, err := http.NewRequest(http.MethodPost, c.baseURL+path, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	return c.do(req, v)
}

// PostRaw performs an authenticated POST with a JSON body and returns raw bytes.
func (c *Client) PostRaw(path string, body any) ([]byte, int, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal body: %w", err)
	}
	req, err := http.NewRequest(http.MethodPost, c.baseURL+path, bytes.NewReader(data))
	if err != nil {
		return nil, 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	return c.doRaw(req)
}

// Delete performs an authenticated DELETE.
func (c *Client) Delete(path string, v any) error {
	req, err := http.NewRequest(http.MethodDelete, c.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	return c.do(req, v)
}

// UploadFile performs a multipart POST to upload a file from disk.
// Returns the JSON response decoded into v.
func (c *Client) UploadFile(path, localPath string, v any) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", localPath, err)
	}
	defer f.Close()

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	part, err := w.CreateFormFile("file", filepath.Base(localPath))
	if err != nil {
		return fmt.Errorf("create form file: %w", err)
	}
	if _, err := io.Copy(part, f); err != nil {
		return fmt.Errorf("copy file: %w", err)
	}
	w.Close()

	req, err := http.NewRequest(http.MethodPost, c.baseURL+path, &buf)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	return c.do(req, v)
}

// PostNoAuth performs an unauthenticated POST
func (c *Client) PostNoAuth(path string, body any, v any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}
	req, err := http.NewRequest(http.MethodPost, c.baseURL+path, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	// Deliberately no Authorization header.
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	return c.decodeResponse(resp, v)
}

// do attaches the bearer token and executes the request.
func (c *Client) do(req *http.Request, v any) error {
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	return c.decodeResponse(resp, v)
}

// doRaw executes the request and returns raw bytes + status code.
func (c *Client) doRaw(req *http.Request) ([]byte, int, error) {
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	if resp.StatusCode >= 400 {
		var errBody struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(raw, &errBody)
		msg := errBody.Error
		if msg == "" {
			msg = string(raw)
		}
		return nil, resp.StatusCode, &APIError{Status: resp.StatusCode, Message: msg}
	}
	return raw, resp.StatusCode, nil
}

// decodeResponse reads the body and unmarshals it into v (if non-nil).
// Returns an APIError for non-2xx responses.
func (c *Client) decodeResponse(resp *http.Response, v any) error {
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		var errBody struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(raw, &errBody)
		msg := errBody.Error
		if msg == "" {
			msg = string(raw)
		}
		return &APIError{Status: resp.StatusCode, Message: msg}
	}

	if v != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, v); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}
