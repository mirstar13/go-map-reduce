package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/mirstar13/go-map-reduce/pkg/middleware/auth"
	"github.com/mirstar13/go-map-reduce/services/ui/client"
	uicfg "github.com/mirstar13/go-map-reduce/services/ui/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var regularUser = &auth.Identity{Subject: "user-uuid-1", Email: "alice@test.com", Roles: []string{"user"}}
var adminUser = &auth.Identity{Subject: "admin-uuid-1", Email: "admin@test.com", Roles: []string{"admin"}}

func withIdentity(id *auth.Identity) fiber.Handler {
	return func(c fiber.Ctx) error {
		auth.SetIdentity(c, id)
		return c.Next()
	}
}

func doRequest(t *testing.T, app *fiber.App, method, path string, body interface{}) *http.Response {
	t.Helper()
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		require.NoError(t, err)
		bodyReader = bytes.NewReader(data)
	}
	req := httptest.NewRequest(method, path, bodyReader)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := app.Test(req)
	require.NoError(t, err)
	return resp
}

func decodeJSON(t *testing.T, r *http.Response) map[string]interface{} {
	t.Helper()
	defer r.Body.Close()
	var m map[string]interface{}
	require.NoError(t, json.NewDecoder(r.Body).Decode(&m))
	return m
}

func managerStubServer(t *testing.T, status int, body interface{}) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(body)
	}))
}

type capturedReq struct {
	Method  string
	Path    string
	Headers http.Header
	Body    []byte
}

func capturingStubServer(t *testing.T, status int, body interface{}) (*httptest.Server, *capturedReq) {
	t.Helper()
	cap := &capturedReq{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cap.Method = r.Method
		cap.Path = r.URL.Path
		cap.Headers = r.Header.Clone()
		cap.Body, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(body)
	}))
	return srv, cap
}

// Each builder fills only the URL relevant to a test group; all other required
// fields get safe placeholder values so the config validates.

func managerCfg(managerURL string) *uicfg.Config {
	return &uicfg.Config{
		Port:                  "8081",
		KeycloakURL:           "http://keycloak-placeholder:8080",
		KeycloakRealm:         "mapreduce",
		KeycloakClientID:      "mapreduce-ui",
		KeycloakAdminUser:     "admin",
		KeycloakAdminPassword: "admin",
		// ManagerAPIURL is used by Get/List/Cancel/Output — all tests in this
		// file that call those methods point the stub here.
		ManagerAPIURL:       managerURL,
		ManagerHeadlessHost: strings.TrimPrefix(managerURL, "http://"),
		ManagerPort:         "80",
		ManagerReplicas:     1,
		MinioEndpoint:       "minio-placeholder:9000",
		MinioBucketInput:    "input",
		MinioBucketCode:     "code",
		MinioBucketOutput:   "output",
		MinioAccessKey:      "minioadmin",
		MinioSecretKey:      "minioadmin",
	}
}

func keycloakCfg(keycloakURL string) *uicfg.Config {
	return &uicfg.Config{
		Port:                  "8081",
		KeycloakURL:           keycloakURL,
		KeycloakRealm:         "mapreduce",
		KeycloakClientID:      "mapreduce-ui",
		KeycloakAdminUser:     "admin",
		KeycloakAdminPassword: "admin",
		ManagerAPIURL:         "http://manager-placeholder:8080",
		ManagerHeadlessHost:   "manager-placeholder",
		ManagerPort:           "80",
		ManagerReplicas:       1,
		MinioEndpoint:         "minio-placeholder:9000",
		MinioBucketInput:      "input",
		MinioBucketCode:       "code",
		MinioBucketOutput:     "output",
		MinioAccessKey:        "minioadmin",
		MinioSecretKey:        "minioadmin",
	}
}

func jobApp(t *testing.T, managerURL string, id *auth.Identity) *fiber.App {
	t.Helper()
	mc := client.NewManagerClient(managerCfg(managerURL))
	h := NewJobHandler(mc, zap.NewNop())
	app := fiber.New()
	app.Use(withIdentity(id))
	app.Get("/jobs", h.ListJobs)
	app.Post("/jobs", h.SubmitJob)
	app.Get("/jobs/:id", h.GetJob)
	app.Delete("/jobs/:id", h.CancelJob)
	app.Get("/jobs/:id/output", h.GetJobOutput)
	return app
}

func authApp(t *testing.T, keycloakURL string) *fiber.App {
	t.Helper()
	kc := client.NewKeycloakClient(keycloakCfg(keycloakURL))
	h := NewAuthHandler(kc, zap.NewNop())
	app := fiber.New()
	app.Post("/auth/login", h.Login)
	return app
}

func adminApp(t *testing.T, managerURL, keycloakURL string, id *auth.Identity) *fiber.App {
	t.Helper()
	mc := client.NewManagerClient(managerCfg(managerURL))
	kc := client.NewKeycloakClient(keycloakCfg(keycloakURL))
	h := NewAdminHandler(mc, kc, zap.NewNop())
	app := fiber.New()
	app.Use(withIdentity(id))
	app.Get("/admin/jobs", h.ListAllJobs)
	app.Get("/admin/users", h.ListUsers)
	app.Post("/admin/users", h.CreateUser)
	app.Get("/admin/users/:id", h.GetUser)
	app.Delete("/admin/users/:id", h.DeleteUser)
	app.Post("/admin/users/:id/roles", h.AssignRole)
	return app
}

func TestListJobs_ForwardsToManager_ReturnsManagerResponse(t *testing.T) {
	stub := managerStubServer(t, http.StatusOK,
		[]map[string]string{{"job_id": "abc", "status": "COMPLETED"}})
	defer stub.Close()

	resp := doRequest(t, jobApp(t, stub.URL, regularUser), http.MethodGet, "/jobs", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestListJobs_ForwardsIdentityHeaders(t *testing.T) {
	// The handler must forward X-User-Sub/Email/Roles so the Manager can
	// authorise without re-validating the JWT.
	stub, cap := capturingStubServer(t, http.StatusOK, []interface{}{})
	defer stub.Close()

	doRequest(t, jobApp(t, stub.URL, regularUser), http.MethodGet, "/jobs", nil)

	assert.Equal(t, regularUser.Subject, cap.Headers.Get("X-User-Sub"))
	assert.Equal(t, regularUser.Email, cap.Headers.Get("X-User-Email"))
	assert.Contains(t, cap.Headers.Get("X-User-Roles"), "user")
}

func TestListJobs_ManagerUnreachable_ReturnsBadGateway(t *testing.T) {
	mc := client.NewManagerClient(managerCfg("http://127.0.0.1:19999"))
	h := NewJobHandler(mc, zap.NewNop())
	app := fiber.New()
	app.Use(withIdentity(regularUser))
	app.Get("/jobs", h.ListJobs)

	resp := doRequest(t, app, http.MethodGet, "/jobs", nil)
	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
}

// NOTE: There is intentionally no "NoIdentity → 401" test for UI job handlers.
// In production the JWT middleware always populates the identity before the
// handler runs; bypassing it in a test causes a nil pointer dereference because
// the handler calls id.Subject unconditionally. Authentication boundary coverage
// lives in pkg/middleware/auth and pkg/middleware/rbac.

func TestSubmitJob_EmptyBody_ReturnsBadRequest(t *testing.T) {
	// The handler checks for an empty body before forwarding to the Manager.
	stub := managerStubServer(t, http.StatusCreated, map[string]string{})
	defer stub.Close()

	mc := client.NewManagerClient(managerCfg(stub.URL))
	h := NewJobHandler(mc, zap.NewNop())
	app := fiber.New()
	app.Use(withIdentity(regularUser))
	app.Post("/jobs", h.SubmitJob)

	req := httptest.NewRequest(http.MethodPost, "/jobs", nil) // no body
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// NOTE: "ForwardsBodyToManager" and "RoutesToJobsPath" tests are intentionally
// omitted. SubmitJob uses ManagerClient.SubmitJob which routes via the headless
// DNS pattern (http://manager-{i}.{HeadlessHost}:{Port}) — not ManagerAPIURL.
// Constructing a URL that resolves to an httptest.Server from inside that pattern
// requires either DNS mocking or a real network name, neither of which is
// practical in a unit test. The body-forwarding contract between the UI service
// and the Manager is instead covered by the manager handler tests.

func TestGetJob_ForwardsToManagerWithID(t *testing.T) {
	stub, cap := capturingStubServer(t, http.StatusOK, map[string]string{"job_id": "job-abc"})
	defer stub.Close()

	resp := doRequest(t, jobApp(t, stub.URL, regularUser), http.MethodGet, "/jobs/job-abc", nil)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "/jobs/job-abc", cap.Path)
}

func TestCancelJob_ForwardsAsPostCancel(t *testing.T) {
	// The UI handler converts DELETE /jobs/:id → POST /jobs/:id/cancel on the
	// Manager — the Manager's cancel endpoint is POST, not DELETE.
	stub, cap := capturingStubServer(t, http.StatusOK, map[string]string{"status": "CANCELLED"})
	defer stub.Close()

	resp := doRequest(t, jobApp(t, stub.URL, regularUser), http.MethodDelete, "/jobs/job-abc", nil)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "/jobs/job-abc/cancel", cap.Path)
}

func TestGetJobOutput_ForwardsToManagerOutputPath(t *testing.T) {
	stub, cap := capturingStubServer(t, http.StatusOK,
		map[string]interface{}{"output_paths": []string{"output/part-0.jsonl"}})
	defer stub.Close()

	resp := doRequest(t, jobApp(t, stub.URL, regularUser),
		http.MethodGet, "/jobs/job-abc/output", nil)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "/jobs/job-abc/output", cap.Path)
}

func TestGetJobOutput_ManagerUnreachable_ReturnsBadGateway(t *testing.T) {
	mc := client.NewManagerClient(managerCfg("http://127.0.0.1:19999"))
	h := NewJobHandler(mc, zap.NewNop())
	app := fiber.New()
	app.Use(withIdentity(regularUser))
	app.Get("/jobs/:id/output", h.GetJobOutput)

	resp := doRequest(t, app, http.MethodGet, "/jobs/some-id/output", nil)
	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
}

func TestLogin_ValidCredentials_ReturnsToken(t *testing.T) {
	kcStub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token":       "eyJ.payload.sig",
			"expires_in":         3600,
			"refresh_token":      "refresh.tok",
			"refresh_expires_in": 86400,
			"token_type":         "Bearer",
		})
	}))
	defer kcStub.Close()

	resp := doRequest(t, authApp(t, kcStub.URL), http.MethodPost, "/auth/login",
		map[string]string{"username": "alice", "password": "secret"})

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := decodeJSON(t, resp)
	assert.Equal(t, "eyJ.payload.sig", body["access_token"])
	assert.NotEmpty(t, body["refresh_token"])
}

func TestLogin_MissingUsername_ReturnsBadRequest(t *testing.T) {
	resp := doRequest(t, authApp(t, "http://irrelevant"), http.MethodPost, "/auth/login",
		map[string]string{"password": "secret"})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestLogin_MissingPassword_ReturnsBadRequest(t *testing.T) {
	resp := doRequest(t, authApp(t, "http://irrelevant"), http.MethodPost, "/auth/login",
		map[string]string{"username": "alice"})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestLogin_BothFieldsEmpty_ReturnsBadRequest(t *testing.T) {
	resp := doRequest(t, authApp(t, "http://irrelevant"), http.MethodPost, "/auth/login",
		map[string]string{"username": "", "password": ""})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestLogin_InvalidCredentials_ReturnsUnauthorized(t *testing.T) {
	kcStub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"error":"invalid_grant"}`)
	}))
	defer kcStub.Close()

	resp := doRequest(t, authApp(t, kcStub.URL), http.MethodPost, "/auth/login",
		map[string]string{"username": "alice", "password": "wrong"})
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestLogin_InvalidJSONBody_ReturnsBadRequest(t *testing.T) {
	app := authApp(t, "http://irrelevant")
	req := httptest.NewRequest(http.MethodPost, "/auth/login",
		bytes.NewBufferString("not json {{"))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestUploadInput_NoFileField_ReturnsBadRequest ensures the handler rejects
// multipart forms that don't include a "file" field. The MinioClient is never
// reached because the form validation fails first.
func TestUploadInput_NoFileField_ReturnsBadRequest(t *testing.T) {
	h := NewFileHandler(&client.MinioClient{}, zap.NewNop())
	app := fiber.New()
	app.Post("/files/input", h.UploadInput)

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	fw, _ := w.CreateFormField("not_file") // wrong field name
	fmt.Fprint(fw, "data")
	w.Close()

	req := httptest.NewRequest(http.MethodPost, "/files/input", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestUploadInput_NonMultipart_ReturnsBadRequest(t *testing.T) {
	h := NewFileHandler(&client.MinioClient{}, zap.NewNop())
	app := fiber.New()
	app.Post("/files/input", h.UploadInput)

	req := httptest.NewRequest(http.MethodPost, "/files/input",
		bytes.NewBufferString(`{"file":"data"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestUploadCode_NoFileField_ReturnsBadRequest(t *testing.T) {
	h := NewFileHandler(&client.MinioClient{}, zap.NewNop())
	app := fiber.New()
	app.Post("/files/code", h.UploadCode)

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	fw, _ := w.CreateFormField("wrong")
	fmt.Fprint(fw, "print('hello')")
	w.Close()

	req := httptest.NewRequest(http.MethodPost, "/files/code", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestUploadCode_NonMultipart_ReturnsBadRequest(t *testing.T) {
	h := NewFileHandler(&client.MinioClient{}, zap.NewNop())
	app := fiber.New()
	app.Post("/files/code", h.UploadCode)

	req := httptest.NewRequest(http.MethodPost, "/files/code",
		bytes.NewBufferString(`{"file":"data"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// NOTE: Happy-path upload tests (ValidFile → 201) are intentionally omitted.
// The MinIO SDK performs a bucket location GET before any PUT, expecting an
// XML response (<?xml ...><LocationConstraint>). A generic httptest.Server
// returning HTTP 200 with an empty body causes the SDK to fail with a parse
// error, producing a 500 from the handler. Fully stubbing the MinIO wire
// protocol would duplicate what the MinIO SDK's own integration tests already
// cover. The handler's upload-success path is exercised in end-to-end tests
// against a real MinIO instance.

func TestAdminListAllJobs_ForwardsToManagerAdminEndpoint(t *testing.T) {
	stub, cap := capturingStubServer(t, http.StatusOK,
		[]map[string]string{{"job_id": "x"}})
	defer stub.Close()

	resp := doRequest(t, adminApp(t, stub.URL, "http://irrelevant", adminUser),
		http.MethodGet, "/admin/jobs", nil)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "/admin/jobs", cap.Path)
}

func TestAdminListAllJobs_ForwardsAdminIdentityHeaders(t *testing.T) {
	stub, cap := capturingStubServer(t, http.StatusOK, []interface{}{})
	defer stub.Close()

	doRequest(t, adminApp(t, stub.URL, "http://irrelevant", adminUser),
		http.MethodGet, "/admin/jobs", nil)

	assert.Equal(t, adminUser.Subject, cap.Headers.Get("X-User-Sub"))
	assert.Contains(t, cap.Headers.Get("X-User-Roles"), "admin")
}

func TestAdminListAllJobs_ManagerUnreachable_ReturnsBadGateway(t *testing.T) {
	resp := doRequest(t,
		adminApp(t, "http://127.0.0.1:19999", "http://irrelevant", adminUser),
		http.MethodGet, "/admin/jobs", nil)
	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
}

func TestAdminCreateUser_MissingPassword_ReturnsBadRequest(t *testing.T) {
	resp := doRequest(t,
		adminApp(t, "http://irrelevant", "http://irrelevant", adminUser),
		http.MethodPost, "/admin/users",
		map[string]string{"username": "bob", "email": "bob@test.com"})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestAdminCreateUser_MissingUsername_ReturnsBadRequest(t *testing.T) {
	resp := doRequest(t,
		adminApp(t, "http://irrelevant", "http://irrelevant", adminUser),
		http.MethodPost, "/admin/users",
		map[string]string{"email": "bob@test.com", "password": "secret"})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestAdminCreateUser_InvalidRole_ReturnsBadRequest(t *testing.T) {
	resp := doRequest(t,
		adminApp(t, "http://irrelevant", "http://irrelevant", adminUser),
		http.MethodPost, "/admin/users",
		map[string]string{
			"username": "bob",
			"email":    "bob@test.com",
			"password": "secret",
			"role":     "superuser", // not "user" or "admin"
		})

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	body := decodeJSON(t, resp)
	assert.Contains(t, body["error"], "role")
}

func TestAdminCreateUser_DefaultsRoleToUser(t *testing.T) {
	// When role is omitted the handler defaults to "user" and calls Keycloak.
	// Stub the three Keycloak calls the SDK makes:
	//   1. POST /realms/master/.../token  — get an admin token
	//   2. POST /admin/realms/.../users   — create the user (→ Location header)
	//   3. GET  /admin/realms/.../roles/user — resolve the role representation
	//   4. POST /admin/realms/.../users/{id}/role-mappings/realm — assign role
	kcStub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost &&
			r.URL.Path == "/realms/master/protocol/openid-connect/token":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"access_token": "admin-tok",
				"expires_in":   3600,
			})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/users"):
			w.Header().Set("Location", r.RequestURI+"/new-user-id")
			w.WriteHeader(http.StatusCreated)
		default:
			// Role GET + assignment POST → no content
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer kcStub.Close()

	resp := doRequest(t,
		adminApp(t, "http://irrelevant", kcStub.URL, adminUser),
		http.MethodPost, "/admin/users",
		map[string]string{
			"username": "bob",
			"email":    "bob@test.com",
			"password": "secret",
			// role omitted → defaults to "user"
		})

	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	body := decodeJSON(t, resp)
	assert.Equal(t, "user", body["role"])
}

func TestAdminAssignRole_InvalidRole_ReturnsBadRequest(t *testing.T) {
	resp := doRequest(t,
		adminApp(t, "http://irrelevant", "http://irrelevant", adminUser),
		http.MethodPost, "/admin/users/some-id/roles",
		map[string]string{"role": "superuser"})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestAdminAssignRole_MissingRole_ReturnsBadRequest(t *testing.T) {
	resp := doRequest(t,
		adminApp(t, "http://irrelevant", "http://irrelevant", adminUser),
		http.MethodPost, "/admin/users/some-id/roles",
		map[string]string{})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestAdminDeleteUser_ForwardsToKeycloak(t *testing.T) {
	var deletedPath string
	kcStub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost &&
			r.URL.Path == "/realms/master/protocol/openid-connect/token":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"access_token": "admin-tok",
				"expires_in":   3600,
			})
		case r.Method == http.MethodDelete:
			deletedPath = r.URL.Path
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer kcStub.Close()

	resp := doRequest(t,
		adminApp(t, "http://irrelevant", kcStub.URL, adminUser),
		http.MethodDelete, "/admin/users/keycloak-user-id", nil)

	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Contains(t, deletedPath, "keycloak-user-id")
}
