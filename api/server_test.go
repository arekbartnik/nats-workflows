package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/arekbartnik/nats-workflows/api"
	"github.com/arekbartnik/nats-workflows/engine"
)

type approvalInput struct {
	Approved bool `json:"approved"`
}

type greetState struct {
	Name     string `json:"name"`
	Greeting string `json:"greeting,omitempty"`
	Approved bool   `json:"approved,omitempty"`
}

func greetWorkflow() engine.Workflow {
	return engine.Workflow{
		Name: "greet",
		Steps: []engine.Step{
			{Name: "prepare", Run: engine.Typed(func(ctx context.Context, run *engine.Run, s *greetState) error {
				s.Greeting = "hello " + s.Name
				return nil
			})},
			{Name: "confirm", Input: engine.HookType[approvalInput]("approval-v1", nil), Run: engine.Typed(func(ctx context.Context, run *engine.Run, s *greetState) error {
				input, err := engine.HookInput[approvalInput](ctx, run, "confirm", "")
				if err != nil {
					return err
				}
				s.Approved = input.Approved
				return nil
			})},
		},
		Result: func(run *engine.Run) string {
			s, _ := engine.StateOf[greetState](run)
			return s.Greeting
		},
	}
}

func startServer(t *testing.T) *httptest.Server {
	t.Helper()
	ns, err := server.NewServer(&server.Options{
		ServerName: "api-test",
		DontListen: true,
		JetStream:  true,
		StoreDir:   t.TempDir(),
	})
	if err != nil {
		t.Fatalf("creating embedded server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("embedded server never became ready")
	}
	nc, err := nats.Connect("", nats.InProcessServer(ns))
	if err != nil {
		t.Fatalf("connecting in-process: %v", err)
	}
	t.Cleanup(func() {
		nc.Close()
		ns.Shutdown()
		ns.WaitForShutdown()
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng, err := engine.New(ctx, nc, log, engine.Config{}, greetWorkflow())
	if err != nil {
		t.Fatalf("creating engine: %v", err)
	}
	go eng.Serve(ctx)

	ts := httptest.NewServer(api.New(eng, log).Handler())
	t.Cleanup(ts.Close)
	return ts
}

func do(t *testing.T, method, url, body string, wantStatus int, out any) {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus {
		t.Fatalf("%s %s: got %d, want %d; body: %s", method, url, resp.StatusCode, wantStatus, data)
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			t.Fatalf("decoding response %s: %v", data, err)
		}
	}
}

type runResp struct {
	Hooks    []*engine.Hook  `json:"hooks"`
	ID       string          `json:"id"`
	Workflow string          `json:"workflow"`
	Status   engine.Status   `json:"status"`
	StepName string          `json:"step_name"`
	Error    string          `json:"error"`
	State    json.RawMessage `json:"state"`
	Result   string          `json:"result"`
}

func TestListWorkflows(t *testing.T) {
	ts := startServer(t)
	var wfs []struct {
		Name  string   `json:"name"`
		Steps []string `json:"steps"`
	}
	do(t, "GET", ts.URL+"/api/v1/workflows", "", http.StatusOK, &wfs)
	if len(wfs) != 1 || wfs[0].Name != "greet" {
		t.Fatalf("got workflows %+v, want [greet]", wfs)
	}
	if len(wfs[0].Steps) != 2 || wfs[0].Steps[0] != "prepare" {
		t.Fatalf("got steps %v, want [prepare confirm]", wfs[0].Steps)
	}
}

func TestStartParkResumeFlow(t *testing.T) {
	ts := startServer(t)

	var run runResp
	do(t, "POST", ts.URL+"/api/v1/runs", `{"workflow":"greet","state":{"name":"leia"}}`,
		http.StatusCreated, &run)
	if run.ID == "" || run.Workflow != "greet" {
		t.Fatalf("unexpected created run: %+v", run)
	}

	do(t, "GET", ts.URL+"/api/v1/runs/"+run.ID+"/wait?timeout=10s", "", http.StatusOK, &run)
	if run.Status != engine.StatusParked || run.StepName != "confirm" {
		t.Fatalf("after wait: %+v, want parked at confirm", run)
	}

	do(t, "POST", ts.URL+"/api/v1/runs/"+run.ID+"/retry", "", http.StatusConflict, nil)

	do(t, "POST", ts.URL+"/api/v1/runs/"+run.ID+"/resume", fmt.Sprintf(`{"token":%q,"submission_id":"approval-1","payload":{"approved":true}}`, run.Hooks[0].Token), http.StatusAccepted, nil)
	do(t, "GET", ts.URL+"/api/v1/runs/"+run.ID+"/wait?timeout=10s", "", http.StatusOK, &run)
	if run.Status != engine.StatusDone {
		t.Fatalf("after resume: %+v, want done", run)
	}
	if run.Result != "hello leia" {
		t.Fatalf("result = %q, want %q", run.Result, "hello leia")
	}

	do(t, "POST", ts.URL+"/api/v1/runs/"+run.ID+"/resume", "", http.StatusBadRequest, nil)
	var runs []runResp
	do(t, "GET", ts.URL+"/api/v1/runs?status=done", "", http.StatusOK, &runs)
	if len(runs) != 1 || runs[0].ID != run.ID {
		t.Fatalf("list done runs = %+v, want the finished run", runs)
	}

	var hist []runResp
	do(t, "GET", ts.URL+"/api/v1/runs/"+run.ID+"/history", "", http.StatusOK, &hist)
	if len(hist) < 4 {
		t.Fatalf("history has %d revisions, want at least 4: %+v", len(hist), hist)
	}
	if first := hist[0]; first.Status != engine.StatusRunning || first.StepName != "prepare" {
		t.Fatalf("first revision = %+v, want running at prepare", first)
	}
	if last := hist[len(hist)-1]; last.Status != engine.StatusDone || last.Result != "hello leia" {
		t.Fatalf("last revision = %+v, want done with result", last)
	}
	var parked bool
	for _, h := range hist {
		parked = parked || h.Status == engine.StatusParked
	}
	if !parked {
		t.Fatalf("no parked revision in history: %+v", hist)
	}
}

func TestErrors(t *testing.T) {
	ts := startServer(t)
	do(t, "POST", ts.URL+"/api/v1/runs", `{"workflow":"nope"}`, http.StatusBadRequest, nil)
	do(t, "POST", ts.URL+"/api/v1/runs", `{}`, http.StatusBadRequest, nil)
	do(t, "GET", ts.URL+"/api/v1/runs/missing", "", http.StatusNotFound, nil)
	do(t, "GET", ts.URL+"/api/v1/runs/missing/history", "", http.StatusNotFound, nil)
	do(t, "POST", ts.URL+"/api/v1/runs/missing/resume", `{"token":"unknown","submission_id":"x","payload":{}}`, http.StatusNotFound, nil)
	do(t, "POST", ts.URL+"/api/v1/runs/missing/retry", "", http.StatusNotFound, nil)
	do(t, "GET", ts.URL+"/api/v1/runs/missing/wait?timeout=2s", "", http.StatusNotFound, nil)
	do(t, "GET", ts.URL+"/api/v1/runs/x/wait?timeout=bogus", "", http.StatusBadRequest, nil)
}

func TestDocsServed(t *testing.T) {
	ts := startServer(t)
	for _, path := range []string{"/openapi.yaml", "/api/docs"} {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: got %d, want 200", path, resp.StatusCode)
		}
	}
}

func TestHookHTTPContract(t *testing.T) {
	ts := startServer(t)
	var run runResp
	do(t, "POST", ts.URL+"/api/v1/runs", `{"workflow":"greet","state":{"name":"luke"}}`, http.StatusCreated, &run)
	do(t, "GET", ts.URL+"/api/v1/runs/"+run.ID+"/wait?timeout=10s", "", http.StatusOK, &run)
	token := run.Hooks[0].Token
	body := fmt.Sprintf(`{"token":%q,"submission_id":"action","payload":{"approved":true}}`, token)
	for _, bad := range []string{
		fmt.Sprintf(`{"token":%q,"payload":{"approved":true}}`, token),
		fmt.Sprintf(`{"token":%q,"submission_id":"action","payload":{"approved":true,"name":"changed"}}`, token),
		body + ` {}`,
	} {
		do(t, "POST", ts.URL+"/api/v1/hooks/submit", bad, http.StatusBadRequest, nil)
	}
	do(t, "POST", ts.URL+"/api/v1/runs/another-run/resume", body, http.StatusConflict, nil)
	var first, again engine.Receipt
	do(t, "POST", ts.URL+"/api/v1/hooks/submit", body, http.StatusAccepted, &first)
	do(t, "GET", ts.URL+"/api/v1/runs/"+run.ID+"/wait?timeout=10s", "", http.StatusOK, &run)
	do(t, "POST", ts.URL+"/api/v1/runs/"+run.ID+"/resume", body, http.StatusAccepted, &again)
	if first != again || run.Status != engine.StatusDone {
		t.Fatalf("receipt or execution: %+v %+v %+v", first, again, run)
	}
	do(t, "POST", ts.URL+"/api/v1/hooks/submit", strings.Replace(body, `"action"`, `"another"`, 1), http.StatusConflict, nil)
}
