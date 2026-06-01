package handler

import (
	"github.com/gofiber/fiber/v3"
	"go.uber.org/zap"

	"github.com/mirstar13/go-map-reduce/pkg/middleware/auth"
	"github.com/mirstar13/go-map-reduce/services/ui/client"
)

// WorkflowHandler handles workflow-related routes.
type WorkflowHandler struct {
	manager *client.ManagerClient
	log     *zap.Logger
}

// NewWorkflowHandler creates a new WorkflowHandler.
func NewWorkflowHandler(manager *client.ManagerClient, log *zap.Logger) *WorkflowHandler {
	return &WorkflowHandler{manager: manager, log: log}
}

// SubmitWorkflow godoc
//
//	POST /workflows
//	Requires: user or admin role.
func (h *WorkflowHandler) SubmitWorkflow(c fiber.Ctx) error {
	id := auth.GetIdentity(c)

	body := c.Body()
	if len(body) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "request body is required"})
	}

	raw, status, err := h.manager.SubmitWorkflow(c.Context(), id.Subject, id.Email, rolesHeader(id), body)
	if err != nil {
		h.log.Error("submit workflow: manager error", zap.String("user", id.Subject), zap.Error(err))
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": "upstream error"})
	}

	h.log.Info("workflow submitted", zap.String("user", id.Subject), zap.Int("manager_status", status))
	return c.Status(status).Send(raw)
}

// GetWorkflow godoc
//
//	GET /workflows/:id
//	Requires: user or admin role.
func (h *WorkflowHandler) GetWorkflow(c fiber.Ctx) error {
	wfID := c.Params("id")
	if wfID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "workflow id is required"})
	}

	id := auth.GetIdentity(c)
	raw, status, err := h.manager.GetWorkflow(c.Context(), wfID, id.Subject, id.Email, rolesHeader(id))
	if err != nil {
		h.log.Error("get workflow: manager error", zap.String("workflow_id", wfID), zap.Error(err))
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": "upstream error"})
	}
	return c.Status(status).Send(raw)
}
