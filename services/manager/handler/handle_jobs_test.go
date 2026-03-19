package handler

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/mirstar13/go-map-reduce/db"
	"github.com/mirstar13/go-map-reduce/pkg/middleware/auth"
	"github.com/mirstar13/go-map-reduce/services/manager/config"
	"github.com/mirstar13/go-map-reduce/services/manager/supervisor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var testCfg = &config.Config{MyReplicaName: "manager-0"}

// withIdentity injects an *auth.Identity into the request context,
// simulating what auth.New / auth.NewInternal would do in production.
func withIdentity(id *auth.Identity) fiber.Handler {
	return func(c fiber.Ctx) error {
		auth.SetIdentity(c, id)
		return c.Next()
	}
}

// noopLaunch is a launchSupervisor stub that does nothing.
func noopLaunch(_ db.Job) {}

// newJobApp wires a fresh Fiber app with the given querier, applying identity
// middleware so handlers can call auth.GetIdentity.
func newJobApp(t *testing.T, q db.Querier, id *auth.Identity) *fiber.App {
	t.Helper()
	h := NewJobHandler(q, supervisor.NewRegistry(), nil, nil, testCfg, zap.NewNop(), noopLaunch)
	app := fiber.New()
	app.Use(withIdentity(id))
	app.Post("/jobs", h.SubmitJob)
	app.Get("/jobs", h.ListJobs)
	app.Get("/jobs/:id", h.GetJob)
	app.Post("/jobs/:id/cancel", h.CancelJob)
	app.Get("/jobs/:id/output", h.GetJobOutput)
	app.Get("/admin/jobs", h.AdminListJobs)
	return app
}

// newJobAppNoIdentity wires the app without any identity (unauthenticated).
func newJobAppNoIdentity(t *testing.T, q db.Querier) *fiber.App {
	t.Helper()
	h := NewJobHandler(q, supervisor.NewRegistry(), nil, nil, testCfg, zap.NewNop(), noopLaunch)
	app := fiber.New()
	app.Post("/jobs", h.SubmitJob)
	app.Get("/jobs", h.ListJobs)
	app.Get("/jobs/:id", h.GetJob)
	app.Post("/jobs/:id/cancel", h.CancelJob)
	app.Get("/jobs/:id/output", h.GetJobOutput)
	return app
}

// doRequest sends a request to app.Test and returns the response.
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

// decodeJSON reads the response body and unmarshals it into a map.
func decodeJSON(t *testing.T, r *http.Response) map[string]interface{} {
	t.Helper()
	defer r.Body.Close()
	var m map[string]interface{}
	require.NoError(t, json.NewDecoder(r.Body).Decode(&m))
	return m
}

// decodeJSONArray reads the response body and unmarshals it into a slice.
func decodeJSONArray(t *testing.T, r *http.Response) []interface{} {
	t.Helper()
	defer r.Body.Close()
	var s []interface{}
	require.NoError(t, json.NewDecoder(r.Body).Decode(&s))
	return s
}

// sampleJob builds a dummy db.Job for use across tests.
func sampleJob(ownerUserID string) db.Job {
	return db.Job{
		JobID:        uuid.New(),
		OwnerUserID:  ownerUserID,
		OwnerReplica: "manager-0",
		Status:       "SUBMITTED",
		MapperPath:   "code/mapper.py",
		ReducerPath:  "code/reducer.py",
		InputPath:    "input/data.jsonl",
		OutputPath:   "output/jobs/abc",
		NumMappers:   4,
		NumReducers:  2,
		InputFormat:  "jsonl",
		SubmittedAt:  time.Now(),
	}
}

func TestSubmitJob_Unauthenticated(t *testing.T) {
	app := newJobAppNoIdentity(t, &mockQuerier{})
	resp := doRequest(t, app, http.MethodPost, "/jobs", map[string]interface{}{
		"mapper_path":  "code/mapper.py",
		"reducer_path": "code/reducer.py",
		"input_path":   "input/data.jsonl",
	})
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestSubmitJob_InvalidBody(t *testing.T) {
	id := &auth.Identity{Subject: "user-1", Roles: []string{"user"}}
	app := newJobApp(t, &mockQuerier{}, id)

	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString("not json"))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestSubmitJob_MissingRequiredFields(t *testing.T) {
	id := &auth.Identity{Subject: "user-1", Roles: []string{"user"}}
	app := newJobApp(t, &mockQuerier{}, id)

	tests := []struct {
		name    string
		payload map[string]interface{}
	}{
		{"missing mapper_path", map[string]interface{}{
			"reducer_path": "code/reducer.py",
			"input_path":   "input/data.jsonl",
		}},
		{"missing reducer_path", map[string]interface{}{
			"mapper_path": "code/mapper.py",
			"input_path":  "input/data.jsonl",
		}},
		{"missing input_path", map[string]interface{}{
			"mapper_path":  "code/mapper.py",
			"reducer_path": "code/reducer.py",
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := doRequest(t, app, http.MethodPost, "/jobs", tc.payload)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			body := decodeJSON(t, resp)
			assert.Contains(t, body["error"], "required")
		})
	}
}

func TestSubmitJob_DBError(t *testing.T) {
	id := &auth.Identity{Subject: "user-1", Roles: []string{"user"}}
	q := &mockQuerier{
		createJobFn: func(_ context.Context, _ db.CreateJobParams) (db.Job, error) {
			return db.Job{}, errors.New("db: connection refused")
		},
	}
	app := newJobApp(t, q, id)
	resp := doRequest(t, app, http.MethodPost, "/jobs", map[string]interface{}{
		"mapper_path":  "code/mapper.py",
		"reducer_path": "code/reducer.py",
		"input_path":   "input/data.jsonl",
	})
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestSubmitJob_Success(t *testing.T) {
	userID := "user-uuid-1"
	id := &auth.Identity{Subject: userID, Email: "alice@test.com", Roles: []string{"user"}}

	createdJob := sampleJob(userID)
	var capturedParams db.CreateJobParams

	q := &mockQuerier{
		createJobFn: func(_ context.Context, arg db.CreateJobParams) (db.Job, error) {
			capturedParams = arg
			return createdJob, nil
		},
	}

	launched := false
	h := NewJobHandler(q, supervisor.NewRegistry(), nil, nil, testCfg, zap.NewNop(), func(_ db.Job) {
		launched = true
	})
	app := fiber.New()
	app.Use(withIdentity(id))
	app.Post("/jobs", h.SubmitJob)

	resp := doRequest(t, app, http.MethodPost, "/jobs", map[string]interface{}{
		"mapper_path":  "code/mapper.py",
		"reducer_path": "code/reducer.py",
		"input_path":   "input/data.jsonl",
		"num_mappers":  4,
		"num_reducers": 2,
		"input_format": "jsonl",
	})

	require.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.True(t, launched, "launchSupervisor must be called after job creation")
	assert.Equal(t, userID, capturedParams.OwnerUserID)
	assert.Equal(t, "manager-0", capturedParams.OwnerReplica)
	assert.Equal(t, "code/mapper.py", capturedParams.MapperPath)
	assert.Equal(t, int32(4), capturedParams.NumMappers)
}

func TestSubmitJob_DefaultsApplied(t *testing.T) {
	// num_mappers < 1 should default to 1; input_format defaults to "jsonl".
	id := &auth.Identity{Subject: "user-1", Roles: []string{"user"}}
	var capturedParams db.CreateJobParams

	q := &mockQuerier{
		createJobFn: func(_ context.Context, arg db.CreateJobParams) (db.Job, error) {
			capturedParams = arg
			return sampleJob("user-1"), nil
		},
	}
	app := newJobApp(t, q, id)
	resp := doRequest(t, app, http.MethodPost, "/jobs", map[string]interface{}{
		"mapper_path":  "code/mapper.py",
		"reducer_path": "code/reducer.py",
		"input_path":   "input/data.jsonl",
		"num_mappers":  0,  // invalid — should be forced to 1
		"num_reducers": -1, // invalid — should be forced to 1
		// input_format intentionally omitted
	})

	require.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.Equal(t, int32(1), capturedParams.NumMappers)
	assert.Equal(t, int32(1), capturedParams.NumReducers)
	assert.Equal(t, "jsonl", capturedParams.InputFormat)
}

func TestListJobs_Unauthenticated(t *testing.T) {
	app := newJobAppNoIdentity(t, &mockQuerier{})
	resp := doRequest(t, app, http.MethodGet, "/jobs", nil)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestListJobs_AdminSeesAll(t *testing.T) {
	id := &auth.Identity{Subject: "admin-1", Roles: []string{"admin"}}

	ownerJobs := []db.Job{sampleJob("alice"), sampleJob("bob")}
	q := &mockQuerier{
		getAllJobsFn: func(_ context.Context) ([]db.Job, error) {
			return ownerJobs, nil
		},
	}
	app := newJobApp(t, q, id)
	resp := doRequest(t, app, http.MethodGet, "/jobs", nil)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	jobs := decodeJSONArray(t, resp)
	assert.Len(t, jobs, 2)
}

func TestListJobs_RegularUserSeesOnlyOwn(t *testing.T) {
	userID := "user-alice"
	id := &auth.Identity{Subject: userID, Roles: []string{"user"}}

	var capturedUserID string
	userJobs := []db.Job{sampleJob(userID)}
	q := &mockQuerier{
		getJobsByUserFn: func(_ context.Context, ownerID string) ([]db.Job, error) {
			capturedUserID = ownerID
			return userJobs, nil
		},
	}
	app := newJobApp(t, q, id)
	resp := doRequest(t, app, http.MethodGet, "/jobs", nil)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, userID, capturedUserID)
	jobs := decodeJSONArray(t, resp)
	assert.Len(t, jobs, 1)
}

func TestListJobs_DBError_Admin(t *testing.T) {
	id := &auth.Identity{Subject: "admin-1", Roles: []string{"admin"}}
	q := &mockQuerier{
		getAllJobsFn: func(_ context.Context) ([]db.Job, error) {
			return nil, errors.New("db failure")
		},
	}
	app := newJobApp(t, q, id)
	resp := doRequest(t, app, http.MethodGet, "/jobs", nil)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestListJobs_DBError_User(t *testing.T) {
	id := &auth.Identity{Subject: "user-1", Roles: []string{"user"}}
	q := &mockQuerier{
		getJobsByUserFn: func(_ context.Context, _ string) ([]db.Job, error) {
			return nil, errors.New("db failure")
		},
	}
	app := newJobApp(t, q, id)
	resp := doRequest(t, app, http.MethodGet, "/jobs", nil)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestGetJob_InvalidID(t *testing.T) {
	id := &auth.Identity{Subject: "user-1", Roles: []string{"user"}}
	app := newJobApp(t, &mockQuerier{}, id)
	resp := doRequest(t, app, http.MethodGet, "/jobs/not-a-uuid", nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestGetJob_NotFound(t *testing.T) {
	id := &auth.Identity{Subject: "user-1", Roles: []string{"user"}}
	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) {
			return db.Job{}, sql.ErrNoRows
		},
	}
	app := newJobApp(t, q, id)
	jobID := uuid.New()
	resp := doRequest(t, app, http.MethodGet, "/jobs/"+jobID.String(), nil)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestGetJob_AccessDenied_OtherUsersJob(t *testing.T) {
	id := &auth.Identity{Subject: "user-alice", Roles: []string{"user"}}
	othersJob := sampleJob("user-bob") // owned by bob

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) {
			return othersJob, nil
		},
	}
	app := newJobApp(t, q, id)
	resp := doRequest(t, app, http.MethodGet, "/jobs/"+othersJob.JobID.String(), nil)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestGetJob_AdminCanSeeAnyJob(t *testing.T) {
	id := &auth.Identity{Subject: "admin-1", Roles: []string{"admin"}}
	othersJob := sampleJob("user-bob")

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) {
			return othersJob, nil
		},
	}
	app := newJobApp(t, q, id)
	resp := doRequest(t, app, http.MethodGet, "/jobs/"+othersJob.JobID.String(), nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestGetJob_OwnerCanSeeOwnJob(t *testing.T) {
	userID := "user-alice"
	id := &auth.Identity{Subject: userID, Roles: []string{"user"}}
	ownJob := sampleJob(userID)

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) {
			return ownJob, nil
		},
	}
	app := newJobApp(t, q, id)
	resp := doRequest(t, app, http.MethodGet, "/jobs/"+ownJob.JobID.String(), nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestGetJob_DBError(t *testing.T) {
	id := &auth.Identity{Subject: "user-1", Roles: []string{"user"}}
	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) {
			return db.Job{}, errors.New("db failure")
		},
	}
	app := newJobApp(t, q, id)
	resp := doRequest(t, app, http.MethodGet, "/jobs/"+uuid.New().String(), nil)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestCancelJob_InvalidID(t *testing.T) {
	id := &auth.Identity{Subject: "user-1", Roles: []string{"user"}}
	app := newJobApp(t, &mockQuerier{}, id)
	resp := doRequest(t, app, http.MethodPost, "/jobs/bad-uuid/cancel", nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestCancelJob_NotFound(t *testing.T) {
	id := &auth.Identity{Subject: "user-1", Roles: []string{"user"}}
	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) {
			return db.Job{}, sql.ErrNoRows
		},
	}
	app := newJobApp(t, q, id)
	resp := doRequest(t, app, http.MethodPost, "/jobs/"+uuid.New().String()+"/cancel", nil)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestCancelJob_AccessDenied(t *testing.T) {
	id := &auth.Identity{Subject: "user-alice", Roles: []string{"user"}}
	othersJob := sampleJob("user-bob")

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) {
			return othersJob, nil
		},
	}
	app := newJobApp(t, q, id)
	resp := doRequest(t, app, http.MethodPost, "/jobs/"+othersJob.JobID.String()+"/cancel", nil)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestCancelJob_Success_Owner(t *testing.T) {
	userID := "user-alice"
	id := &auth.Identity{Subject: userID, Roles: []string{"user"}}
	job := sampleJob(userID)
	cancelCalled := false

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) {
			return job, nil
		},
		cancelJobFn: func(_ context.Context, jid uuid.UUID) error {
			cancelCalled = true
			assert.Equal(t, job.JobID, jid)
			return nil
		},
	}
	app := newJobApp(t, q, id)
	resp := doRequest(t, app, http.MethodPost, "/jobs/"+job.JobID.String()+"/cancel", nil)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, cancelCalled)

	body := decodeJSON(t, resp)
	assert.Equal(t, "CANCELLED", body["status"])
}

func TestCancelJob_Success_Admin(t *testing.T) {
	id := &auth.Identity{Subject: "admin-1", Roles: []string{"admin"}}
	othersJob := sampleJob("user-bob")

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) {
			return othersJob, nil
		},
		cancelJobFn: func(_ context.Context, _ uuid.UUID) error {
			return nil
		},
	}
	app := newJobApp(t, q, id)
	resp := doRequest(t, app, http.MethodPost, "/jobs/"+othersJob.JobID.String()+"/cancel", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestCancelJob_DBCancelError(t *testing.T) {
	userID := "user-alice"
	id := &auth.Identity{Subject: userID, Roles: []string{"user"}}
	job := sampleJob(userID)

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) {
			return job, nil
		},
		cancelJobFn: func(_ context.Context, _ uuid.UUID) error {
			return errors.New("db failure")
		},
	}
	app := newJobApp(t, q, id)
	resp := doRequest(t, app, http.MethodPost, "/jobs/"+job.JobID.String()+"/cancel", nil)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestGetJobOutput_InvalidID(t *testing.T) {
	id := &auth.Identity{Subject: "user-1", Roles: []string{"user"}}
	app := newJobApp(t, &mockQuerier{}, id)
	resp := doRequest(t, app, http.MethodGet, "/jobs/not-a-uuid/output", nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestGetJobOutput_NotFound(t *testing.T) {
	id := &auth.Identity{Subject: "user-1", Roles: []string{"user"}}
	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) {
			return db.Job{}, sql.ErrNoRows
		},
	}
	app := newJobApp(t, q, id)
	resp := doRequest(t, app, http.MethodGet, "/jobs/"+uuid.New().String()+"/output", nil)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestGetJobOutput_AccessDenied(t *testing.T) {
	id := &auth.Identity{Subject: "user-alice", Roles: []string{"user"}}
	othersJob := sampleJob("user-bob")
	othersJob.Status = "COMPLETED"

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) {
			return othersJob, nil
		},
	}
	app := newJobApp(t, q, id)
	resp := doRequest(t, app, http.MethodGet, "/jobs/"+othersJob.JobID.String()+"/output", nil)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestGetJobOutput_JobNotCompleted(t *testing.T) {
	userID := "user-alice"
	id := &auth.Identity{Subject: userID, Roles: []string{"user"}}
	job := sampleJob(userID)
	job.Status = "MAP_PHASE"

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) {
			return job, nil
		},
	}
	app := newJobApp(t, q, id)
	resp := doRequest(t, app, http.MethodGet, "/jobs/"+job.JobID.String()+"/output", nil)
	assert.Equal(t, http.StatusConflict, resp.StatusCode)

	body := decodeJSON(t, resp)
	assert.Equal(t, "MAP_PHASE", body["status"])
}

func TestGetJobOutput_Success(t *testing.T) {
	userID := "user-alice"
	id := &auth.Identity{Subject: userID, Roles: []string{"user"}}
	job := sampleJob(userID)
	job.Status = "COMPLETED"

	paths := []db.GetReduceTaskOutputPathsRow{
		{TaskIndex: 0, OutputPath: sql.NullString{String: "output/jobs/abc/part-0.jsonl", Valid: true}},
		{TaskIndex: 1, OutputPath: sql.NullString{String: "output/jobs/abc/part-1.jsonl", Valid: true}},
	}

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) {
			return job, nil
		},
		getReduceTaskOutputPathsFn: func(_ context.Context, _ uuid.UUID) ([]db.GetReduceTaskOutputPathsRow, error) {
			return paths, nil
		},
	}
	app := newJobApp(t, q, id)
	resp := doRequest(t, app, http.MethodGet, "/jobs/"+job.JobID.String()+"/output", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body := decodeJSON(t, resp)
	assert.NotNil(t, body["output_paths"])
	assert.Equal(t, job.JobID.String(), body["job_id"])
}

func TestGetJobOutput_PathsDBError(t *testing.T) {
	userID := "user-alice"
	id := &auth.Identity{Subject: userID, Roles: []string{"user"}}
	job := sampleJob(userID)
	job.Status = "COMPLETED"

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) {
			return job, nil
		},
		getReduceTaskOutputPathsFn: func(_ context.Context, _ uuid.UUID) ([]db.GetReduceTaskOutputPathsRow, error) {
			return nil, errors.New("db failure")
		},
	}
	app := newJobApp(t, q, id)
	resp := doRequest(t, app, http.MethodGet, "/jobs/"+job.JobID.String()+"/output", nil)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestAdminListJobs_Success(t *testing.T) {
	id := &auth.Identity{Subject: "admin-1", Roles: []string{"admin"}}
	allJobs := []db.Job{sampleJob("alice"), sampleJob("bob"), sampleJob("charlie")}

	q := &mockQuerier{
		getAllJobsFn: func(_ context.Context) ([]db.Job, error) {
			return allJobs, nil
		},
	}
	app := newJobApp(t, q, id)
	resp := doRequest(t, app, http.MethodGet, "/admin/jobs", nil)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	jobs := decodeJSONArray(t, resp)
	assert.Len(t, jobs, 3)
}

func TestAdminListJobs_DBError(t *testing.T) {
	id := &auth.Identity{Subject: "admin-1", Roles: []string{"admin"}}
	q := &mockQuerier{
		getAllJobsFn: func(_ context.Context) ([]db.Job, error) {
			return nil, errors.New("db failure")
		},
	}
	app := newJobApp(t, q, id)
	resp := doRequest(t, app, http.MethodGet, "/admin/jobs", nil)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestCancelJob_GetJobDBError(t *testing.T) {
	id := &auth.Identity{Subject: "user-1", Roles: []string{"user"}}
	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) {
			return db.Job{}, errors.New("db failure")
		},
	}
	app := newJobApp(t, q, id)
	resp := doRequest(t, app, http.MethodPost, "/jobs/"+uuid.New().String()+"/cancel", nil)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestGetJobOutput_GetJobDBError(t *testing.T) {
	id := &auth.Identity{Subject: "user-1", Roles: []string{"user"}}
	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) {
			return db.Job{}, errors.New("db failure")
		},
	}
	app := newJobApp(t, q, id)
	resp := doRequest(t, app, http.MethodGet, "/jobs/"+uuid.New().String()+"/output", nil)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestRolesContain(t *testing.T) {
	tests := []struct {
		name  string
		roles []string
		role  string
		want  bool
	}{
		{"match exact", []string{"admin", "user"}, "admin", true},
		{"match case-insensitive", []string{"Admin"}, "admin", true},
		{"match with whitespace", []string{" user "}, "user", true},
		{"no match", []string{"user"}, "admin", false},
		{"empty roles", []string{}, "admin", false},
		{"empty target", []string{"admin"}, "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, rolesContain(tc.roles, tc.role))
		})
	}
}

func TestAssertAccess_OwnerPasses(t *testing.T) {
	app := fiber.New()
	job := sampleJob("user-alice")
	id := &auth.Identity{Subject: "user-alice", Roles: []string{"user"}}
	app.Get("/check", withIdentity(id), func(c fiber.Ctx) error {
		if err := assertAccess(c, job); err != nil {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": err.Error()})
		}
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/check", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAssertAccess_StrangerFails(t *testing.T) {
	app := fiber.New()
	job := sampleJob("user-bob")
	id := &auth.Identity{Subject: "user-alice", Roles: []string{"user"}}
	app.Get("/check", withIdentity(id), func(c fiber.Ctx) error {
		if err := assertAccess(c, job); err != nil {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": err.Error()})
		}
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/check", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestAssertAccess_NoIdentity(t *testing.T) {
	app := fiber.New()
	job := sampleJob("user-bob")
	app.Get("/check", func(c fiber.Ctx) error {
		if err := assertAccess(c, job); err != nil {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": err.Error()})
		}
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/check", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}
