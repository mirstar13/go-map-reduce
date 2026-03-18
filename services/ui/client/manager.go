package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"time"

	"github.com/mirstar13/go-map-reduce/services/ui/config"
)

// ManagerClient sends requests to the Manager service.
//
// For job submission, it hashes the caller's user ID to consistently route
// to the same StatefulSet replica (e.g. manager-0, manager-1 …).
// For all other operations (list, get, cancel) it uses the ClusterIP service URL,
// allowing Kubernetes to load-balance normally.
type ManagerClient struct {
	cfg        *config.Config
	httpClient *http.Client
}

// NewManagerClient creates a ready-to-use ManagerClient.
func NewManagerClient(cfg *config.Config) *ManagerClient {
	return &ManagerClient{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// replicaURL returns the DNS address of a specific Manager StatefulSet pod.
// Pod DNS pattern: manager-{i}.{headlessHost}:{port}
func (m *ManagerClient) replicaURL(userID string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(userID))
	idx := int(h.Sum32()) % m.cfg.ManagerReplicas
	return fmt.Sprintf("http://manager-%d.%s:%s", idx, m.cfg.ManagerHeadlessHost, m.cfg.ManagerPort)
}

// baseURL returns the ClusterIP service URL for non-submission requests.
func (m *ManagerClient) baseURL() string {
	return m.cfg.ManagerAPIURL
}

// SubmitJob forwards a job-submission request to the hashed Manager replica.
// userID is used as the hash key so the same user always hits the same replica.
// The body is the raw JSON payload from the client; the caller's identity headers
// are forwarded so the Manager can record the owner.
func (m *ManagerClient) SubmitJob(
	ctx context.Context,
	userID, userEmail, userRoles string,
	body []byte,
) (json.RawMessage, int, error) {
	url := m.replicaURL(userID) + "/jobs"
	return m.do(ctx, http.MethodPost, url, userID, userEmail, userRoles, body)
}

// GetJob fetches a single job from the Manager's ClusterIP service.
func (m *ManagerClient) GetJob(
	ctx context.Context,
	jobID, userID, userEmail, userRoles string,
) (json.RawMessage, int, error) {
	url := m.baseURL() + "/jobs/" + jobID
	return m.do(ctx, http.MethodGet, url, userID, userEmail, userRoles, nil)
}

// ListJobs fetches the job list from the Manager's ClusterIP service.
// The Manager uses the X-User-* headers to filter by owner (regular user)
// or return all jobs (admin).
func (m *ManagerClient) ListJobs(
	ctx context.Context,
	userID, userEmail, userRoles string,
) (json.RawMessage, int, error) {
	url := m.baseURL() + "/jobs"
	return m.do(ctx, http.MethodGet, url, userID, userEmail, userRoles, nil)
}

// CancelJob asks the Manager to cancel a job.
func (m *ManagerClient) CancelJob(
	ctx context.Context,
	jobID, userID, userEmail, userRoles string,
) (json.RawMessage, int, error) {
	url := m.baseURL() + "/jobs/" + jobID + "/cancel"
	return m.do(ctx, http.MethodPost, url, userID, userEmail, userRoles, nil)
}

// AdminListJobs returns all jobs regardless of owner (admin only).
func (m *ManagerClient) AdminListJobs(
	ctx context.Context,
	userID, userEmail, userRoles string,
) (json.RawMessage, int, error) {
	url := m.baseURL() + "/admin/jobs"
	return m.do(ctx, http.MethodGet, url, userID, userEmail, userRoles, nil)
}

// GetJobOutput returns the output file paths / presigned URLs for a completed job.
func (m *ManagerClient) GetJobOutput(
	ctx context.Context,
	jobID, userID, userEmail, userRoles string,
) (json.RawMessage, int, error) {
	url := m.baseURL() + "/jobs/" + jobID + "/output"
	return m.do(ctx, http.MethodGet, url, userID, userEmail, userRoles, nil)
}

// do executes an HTTP request against the Manager, forwarding the trusted
// X-User-* headers so the Manager can authorise without re-validating the JWT.
func (m *ManagerClient) do(
	ctx context.Context,
	method, url string,
	userID, userEmail, userRoles string,
	body []byte,
) (json.RawMessage, int, error) {
	var bodyReader io.Reader
	if len(body) > 0 {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, 0, fmt.Errorf("manager client: build request: %w", err)
	}

	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("X-User-Sub", userID)
	req.Header.Set("X-User-Email", userEmail)
	req.Header.Set("X-User-Roles", userRoles)

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("manager client: %s %s: %w", method, url, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("manager client: read response body: %w", err)
	}

	return json.RawMessage(raw), resp.StatusCode, nil
}
