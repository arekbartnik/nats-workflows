package api

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arekbartnik/nats-workflows/engine"
)

//go:embed openapi.yaml
var openapiYAML []byte

//go:embed docs.html
var docsHTML []byte

const (
	waitDefault = 30 * time.Second
	waitMax     = 5 * time.Minute
)

type Server struct {
	eng *engine.Engine
	log *slog.Logger
}

func New(eng *engine.Engine, log *slog.Logger) *Server {
	return &Server{eng: eng, log: log}
}

func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)

	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/workflows", s.listWorkflows)
		r.Get("/runs", s.listRuns)
		r.Post("/runs", s.startRun)
		r.Post("/hooks/submit", s.submitHook)
		r.Route("/runs/{id}", func(r chi.Router) {
			r.Get("/", s.getRun)
			r.Get("/history", s.runHistory)
			r.Post("/resume", s.resumeRun)
			r.Post("/retry", s.retryRun)
			r.Post("/cancel", s.cancelRun)
			r.Post("/reschedule", s.rescheduleRun)
			r.Get("/wait", s.waitRun)
		})
	})
	r.Get("/openapi.yaml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		w.Write(openapiYAML)
	})
	r.Get("/api/docs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(docsHTML)
	})
	r.Get("/api/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return r
}

type workflowInfo struct {
	Name  string   `json:"name"`
	Steps []string `json:"steps"`
}

type runInfo struct {
	Hooks     []*engine.Hook  `json:"hooks"`
	ID        string          `json:"id"`
	Workflow  string          `json:"workflow"`
	Status    engine.Status   `json:"status"`
	Step      int             `json:"step"`
	StepName  string          `json:"step_name"`
	Error     string          `json:"error,omitempty"`
	State     json.RawMessage `json:"state,omitempty"`
	Result    string          `json:"result,omitempty"`
	Updated   time.Time       `json:"updated"`
	StartAt   *time.Time      `json:"start_at,omitempty"`
	ExpiresAt *time.Time      `json:"expires_at,omitempty"`
}

func (s *Server) runInfo(run *engine.Run) runInfo {
	info := runInfo{
		Hooks:     run.Hooks,
		ID:        run.ID,
		Workflow:  run.Workflow,
		Status:    run.Status,
		Step:      run.Step,
		StepName:  s.eng.StepName(run),
		Error:     run.Error,
		State:     run.State,
		Updated:   run.Updated,
		StartAt:   run.StartAt,
		ExpiresAt: run.ExpiresAt,
	}
	if info.Hooks == nil {
		info.Hooks = []*engine.Hook{}
	}
	if run.Status == engine.StatusDone {
		info.Result = s.eng.Result(run)
	}
	return info
}

func (s *Server) listWorkflows(w http.ResponseWriter, r *http.Request) {
	infos := []workflowInfo{}
	for _, wf := range s.eng.Workflows() {
		steps := make([]string, len(wf.Steps))
		for i, st := range wf.Steps {
			steps[i] = st.Name
		}
		infos = append(infos, workflowInfo{Name: wf.Name, Steps: steps})
	}
	writeJSON(w, http.StatusOK, infos)
}

type startRequest struct {
	StartAt  *time.Time      `json:"start_at,omitempty"`
	Workflow string          `json:"workflow"`
	State    json.RawMessage `json:"state,omitempty"`
}

func (s *Server) startRun(w http.ResponseWriter, r *http.Request) {
	var req startRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Workflow == "" {
		writeError(w, http.StatusBadRequest, errors.New("workflow is required"))
		return
	}
	var initial any
	if len(req.State) > 0 {
		initial = req.State
	}
	var id string
	var err error
	if req.StartAt != nil {
		id, err = s.eng.Schedule(r.Context(), req.Workflow, initial, *req.StartAt)
	} else {
		id, err = s.eng.Start(r.Context(), req.Workflow, initial)
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	run, err := s.eng.GetRun(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Location", "/api/v1/runs/"+id)
	writeJSON(w, http.StatusCreated, s.runInfo(run))
}

func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := s.eng.ListRuns(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	status, workflow := r.URL.Query().Get("status"), r.URL.Query().Get("workflow")
	infos := []runInfo{}
	for _, run := range runs {
		if status != "" && string(run.Status) != status {
			continue
		}
		if workflow != "" && run.Workflow != workflow {
			continue
		}
		infos = append(infos, s.runInfo(run))
	}
	writeJSON(w, http.StatusOK, infos)
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	run, err := s.eng.GetRun(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, statusOf(err), err)
		return
	}
	writeJSON(w, http.StatusOK, s.runInfo(run))
}

func (s *Server) runHistory(w http.ResponseWriter, r *http.Request) {
	runs, err := s.eng.RunHistory(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, statusOf(err), err)
		return
	}
	infos := make([]runInfo, len(runs))
	for i, run := range runs {
		infos[i] = s.runInfo(run)
	}
	writeJSON(w, http.StatusOK, infos)
}

func (s *Server) resumeRun(w http.ResponseWriter, r *http.Request) {
	s.acceptHook(w, r, true)
}
func (s *Server) submitHook(w http.ResponseWriter, r *http.Request) {
	s.acceptHook(w, r, false)
}
func (s *Server) acceptHook(w http.ResponseWriter, r *http.Request, scoped bool) {
	var req engine.Submission
	r.Body = http.MaxBytesReader(w, r.Body, int64(s.eng.HookPayloadLimit())+8192)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		writeError(w, http.StatusBadRequest, errors.New("expected one JSON value"))
		return
	}
	var receipt *engine.Receipt
	var err error
	if scoped {
		receipt, err = s.eng.Resume(r.Context(), r.PathValue("id"), req)
	} else {
		receipt, err = s.eng.Submit(r.Context(), req)
	}
	if err != nil {
		status := statusOf(err)
		if status == http.StatusConflict && !errors.Is(err, engine.ErrHookConflict) {
			status = http.StatusInternalServerError
		}
		writeError(w, status, err)
		return
	}
	writeJSON(w, http.StatusAccepted, receipt)
}

func (s *Server) retryRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.eng.Retry(r.Context(), id); err != nil {
		writeError(w, statusOf(err), err)
		return
	}
	s.respondRun(w, r, id)
}

func (s *Server) cancelRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.eng.Cancel(r.Context(), id); err != nil {
		writeError(w, statusOf(err), err)
		return
	}
	s.respondRun(w, r, id)
}

func (s *Server) rescheduleRun(w http.ResponseWriter, r *http.Request) {
	var req struct {
		StartAt time.Time `json:"start_at"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	id := r.PathValue("id")
	if err := s.eng.Reschedule(r.Context(), id, req.StartAt); err != nil {
		writeError(w, statusOf(err), err)
		return
	}
	s.respondRun(w, r, id)
}

func (s *Server) waitRun(w http.ResponseWriter, r *http.Request) {
	timeout := waitDefault
	if q := r.URL.Query().Get("timeout"); q != "" {
		d, err := time.ParseDuration(q)
		if err != nil || d <= 0 {
			writeError(w, http.StatusBadRequest, fmt.Errorf("invalid timeout %q", q))
			return
		}
		timeout = min(d, waitMax)
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	id := r.PathValue("id")
	run, err := s.eng.Wait(ctx, id, engine.StatusParked, engine.StatusDone, engine.StatusFailed, engine.StatusCancelled)
	if err == nil || run != nil && errors.Is(err, context.DeadlineExceeded) {
		writeJSON(w, http.StatusOK, s.runInfo(run))
		return
	}
	s.respondRun(w, r, id)
}

func (s *Server) respondRun(w http.ResponseWriter, r *http.Request, id string) {
	run, err := s.eng.GetRun(r.Context(), id)
	if err != nil {
		writeError(w, statusOf(err), err)
		return
	}
	writeJSON(w, http.StatusOK, s.runInfo(run))
}

func statusOf(err error) int {
	if errors.Is(err, engine.ErrHookGone) {
		return http.StatusGone
	}
	if errors.Is(err, engine.ErrHookCapacity) {
		return http.StatusServiceUnavailable
	}
	if errors.Is(err, engine.ErrHookValidation) {
		return http.StatusBadRequest
	}
	if errors.Is(err, engine.ErrStartTime) {
		return http.StatusBadRequest
	}
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return http.StatusNotFound
	}
	return http.StatusConflict
}

func decodeJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
