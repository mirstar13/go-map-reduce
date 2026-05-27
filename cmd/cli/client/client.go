package client

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
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
			// Large uploads can take much longer than 60s.
			// Timeouts should ideally be handled via context.
			Timeout: 0,
		},
	}
}

// NewInsecure creates a Client that skips TLS certificate verification.
func NewInsecure(baseURL, token string) *Client {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	return &Client{
		baseURL: baseURL,
		token:   token,
		httpClient: &http.Client{
			Timeout:   0,
			Transport: tr,
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

// GetRaw performs an authenticated GET and returns the raw response body bytes.
func (c *Client) GetRaw(path string) ([]byte, int, error) {
	req, err := http.NewRequest(http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("build request: %w", err)
	}
	return c.doRaw(req)
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

// progressReader wraps an io.Reader and calls onProgress after each Read.
type progressReader struct {
	reader     io.Reader
	onProgress func(n int64)
}

func (pr *progressReader) Read(p []byte) (int, error) {
	n, err := pr.reader.Read(p)
	if n > 0 && pr.onProgress != nil {
		pr.onProgress(int64(n))
	}
	return n, err
}

// UploadFile performs a multipart POST to upload a file from disk using streaming.
// This prevents loading the entire file into memory.
func (c *Client) UploadFile(path, localPath string, v any) error {
	return c.UploadFileWithProgress(path, localPath, v, nil)
}

// UploadFileWithProgress is like UploadFile but invokes onProgress with the
// number of bytes just read from disk on every chunk. Callers can use this to
// drive a progress bar.
func (c *Client) UploadFileWithProgress(path, localPath string, v any, onProgress func(n int64)) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", localPath, err)
	}
	// We don't defer close here because we close it in the goroutine

	// Create a pipe to stream the multipart data
	pr, pw := io.Pipe()
	w := multipart.NewWriter(pw)

	go func() {
		defer pw.Close()
		defer f.Close()

		part, err := w.CreateFormFile("file", filepath.Base(localPath))
		if err != nil {
			return
		}

		var reader io.Reader = f
		if onProgress != nil {
			reader = &progressReader{reader: f, onProgress: onProgress}
		}

		if _, err := io.Copy(part, reader); err != nil {
			return
		}
		w.Close()
	}()

	req, err := http.NewRequest(http.MethodPost, c.baseURL+path, pr)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", w.FormDataContentType())

	// Execute the request. The pw.Close() in the goroutine will signal the end of the body.
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
