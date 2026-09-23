package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/arekbartnik/nats-workflows/api"
	"github.com/arekbartnik/nats-workflows/engine"
	"github.com/arekbartnik/nats-workflows/examples/workflows"
)

// Keep os.Exit outside realMain so deferred NATS cleanup runs on errors.
func main() {
	os.Exit(realMain())
}

func realMain() (code int) {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	loadDotEnv(".env")

	cmd := "serve"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}

	nc, shutdown, err := startNATS(log, cmd == "serve")
	if err != nil {
		log.Error("starting NATS", "err", err)
		return 1
	}
	defer shutdown()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	eng, err := engine.New(ctx, nc, log, engine.Config{Namespace: os.Getenv("WORKFLOW_NAMESPACE")},
		workflows.Swapi(), workflows.SwapiChoose(), workflows.Ask(), workflows.OrderPipeline())
	if err != nil {
		log.Error("creating engine", "err", err)
		return 1
	}
	workerDone := make(chan struct{})
	var workerErr error
	go func() {
		workerErr = eng.Serve(ctx)
		if workerErr != nil {
			cancel()
		}
		close(workerDone)
	}()
	defer func() {
		cancel()
		<-workerDone
		if workerErr != nil {
			log.Error("workflow worker", "err", workerErr)
			code = 1
		}
	}()
	var commandArgs []string
	if len(os.Args) > 2 {
		commandArgs = os.Args[2:]
	}
	if handled, err := scheduledCommand(ctx, eng, cmd, commandArgs); handled {
		if err != nil {
			log.Error("scheduling", "err", err)
			return 1
		}
		return 0
	}

	switch cmd {
	case "serve":
		addr, url := httpConfig()
		srv := &http.Server{Addr: addr, Handler: api.New(eng, log).Handler()}
		httpDone := make(chan error, 1)
		go func() {
			httpDone <- srv.ListenAndServe()
		}()
		log.Info("serving; press Ctrl-C to stop", "http", addr, "docs", url+"/api/docs")
		select {
		case <-ctx.Done():
		case err := <-httpDone:
			if !errors.Is(err, http.ErrServerClosed) {
				log.Error("http server", "err", err)
				code = 1
			}
		}
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutCancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			log.Error("http shutdown", "err", err)
			srv.Close()
			return 1
		}
		return code
	case "list":
		if err := listRuns(ctx, eng); err != nil {
			log.Error("listing runs", "err", err)
			return 1
		}
		return 0
	case "retry":
		if len(os.Args) < 3 {
			log.Error("usage: retry <runID>")
			return 2
		}
		if err := retryRun(ctx, eng, os.Args[2]); err != nil {
			log.Error("retrying run", "err", err)
			return 1
		}
		return 0
	case "order":
		if err := runOrder(ctx, eng, os.Args[2:]); err != nil {
			log.Error("order run", "err", err)
			return 1
		}
		return 0
	case "approve":
		if len(os.Args) < 3 {
			log.Error("usage: approve <runID> [approve|reject]")
			return 2
		}
		if err := approveRun(ctx, eng, os.Args[2], os.Args[3:]); err != nil {
			log.Error("approving run", "err", err)
			return 1
		}
		return 0
	}

	var run *engine.Run
	switch cmd {
	case "choose":
		run, err = runChoose(ctx, eng, os.Args[2:])
	case "random":
		run, err = runRandom(ctx, eng)
	case "ask":
		run, err = runAsk(ctx, eng, os.Args[2:])
	default:
		log.Error("unknown command; want serve, random, choose, ask, order, approve, retry or list", "cmd", cmd)
		return 2
	}
	if err != nil {
		log.Error("workflow", "err", err)
		return 1
	}
	if run.Status == engine.StatusFailed {
		log.Error("run failed", "run", run.ID, "err", run.Error)
		return 1
	}
	fmt.Println(eng.Result(run))
	return 0
}

func resumeCommand(workflow string) string {
	switch workflow {
	case "swapi-choose":
		return "choose"
	case "order":
		return "approve"
	default:
		return "resume"
	}
}

func listRuns(ctx context.Context, eng *engine.Engine) error {
	runs, err := eng.ListRuns(ctx)
	if err != nil {
		return err
	}
	if len(runs) == 0 {
		fmt.Println("no runs")
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "RUN\tWORKFLOW\tSTATUS\tSTEP\tUPDATED\tSTART AT")
	for _, r := range runs {
		at := "-"
		if r.StartAt != nil {
			at = r.StartAt.Format(time.RFC3339Nano)
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
			r.ID, r.Workflow, r.Status, eng.StepName(r),
			r.Updated.Local().Format("2006-01-02 15:04:05"), at)
		hooks, err := eng.RunHooks(ctx, r)
		if err != nil {
			return err
		}
		for _, h := range hooks {
			accepted := "-"
			if h.Receipt != nil {
				accepted = h.Receipt.AcceptedAt.Format(time.RFC3339Nano)
			}
			fmt.Fprintf(w, "  hook %s\t%s\t%s\taccepted %s\n", h.Name, h.Token, h.Status, accepted)
		}
	}
	return w.Flush()
}

func retryRun(ctx context.Context, eng *engine.Engine, runID string) error {
	if err := eng.Retry(ctx, runID); err != nil {
		return err
	}
	return followRun(ctx, eng, runID)
}

func runOrder(ctx context.Context, eng *engine.Engine, args []string) error {
	amount := 250
	if len(args) > 0 {
		var err error
		if amount, err = strconv.Atoi(args[0]); err != nil {
			return fmt.Errorf("amount must be a number: %w", err)
		}
	}
	runID, err := eng.Start(ctx, "order", workflows.OrderState{
		Order: workflows.Order{Item: "starship model kit", Qty: 1, Amount: amount},
	})
	if err != nil {
		return err
	}
	return followRun(ctx, eng, runID)
}

func approveRun(ctx context.Context, eng *engine.Engine, runID string, args []string) error {
	decision := "approve"
	if len(args) > 0 {
		if args[0] != "approve" && args[0] != "reject" {
			return fmt.Errorf("decision must be approve or reject, got %q", args[0])
		}
		decision = args[0]
	}
	input := workflows.DecisionInput{Decision: decision}
	if err := submitCLI(ctx, eng, runID, input, args); err != nil {
		return err
	}
	return followRun(ctx, eng, runID)
}

func followRun(ctx context.Context, eng *engine.Engine, runID string) error {
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	run, err := eng.Wait(waitCtx, runID, engine.StatusParked, engine.StatusDone, engine.StatusFailed)
	if err != nil {
		return err
	}
	switch run.Status {
	case engine.StatusFailed:
		return fmt.Errorf("run %s failed: %s", runID, run.Error)
	case engine.StatusParked:
		fmt.Printf("run %s is parked at %q — continue with: go run ./cmd/nats-workflows %s %s\n",
			runID, eng.StepName(run), resumeCommand(run.Workflow), runID)
	default:
		fmt.Println(eng.Result(run))
	}
	return nil
}

func runRandom(ctx context.Context, eng *engine.Engine) (*engine.Run, error) {
	runID, err := eng.Start(ctx, "swapi", nil)
	if err != nil {
		return nil, err
	}
	waitCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	return eng.Wait(waitCtx, runID)
}

func runAsk(ctx context.Context, eng *engine.Engine, args []string) (*engine.Run, error) {
	question := strings.TrimSpace(strings.Join(args, " "))
	if question == "" {
		fmt.Print("question: ")
		line, err := bufio.NewReader(os.Stdin).ReadString('\n')
		if err != nil {
			return nil, err
		}
		question = strings.TrimSpace(line)
	}
	if question == "" {
		return nil, errors.New("empty question")
	}
	runID, err := eng.Start(ctx, "ask", workflows.AskState{Question: question})
	if err != nil {
		return nil, err
	}
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	return eng.Wait(waitCtx, runID)
}

func runChoose(ctx context.Context, eng *engine.Engine, args []string) (*engine.Run, error) {
	var runID string
	if len(args) > 0 {
		runID = args[0]
	} else {
		var err error
		if runID, err = eng.Start(ctx, "swapi-choose", nil); err != nil {
			return nil, err
		}
	}

	waitCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	run, err := eng.Wait(waitCtx, runID, engine.StatusParked, engine.StatusDone, engine.StatusFailed)
	if err != nil || run.Status != engine.StatusParked {
		return run, err
	}

	s, err := engine.StateOf[workflows.SwapiState](run)
	if err != nil {
		return nil, err
	}
	for i, p := range s.People {
		fmt.Printf("%3d. %s\n", i+1, p.Name)
	}
	fmt.Printf("\nrun %s is parked — choose a person (1-%d): ", runID, len(s.People))

	var choice int
	if _, err := fmt.Fscan(os.Stdin, &choice); err != nil {
		return nil, fmt.Errorf("run %s stays parked; resume with: go run ./cmd/nats-workflows choose %s (%w)",
			runID, runID, err)
	}
	if choice < 1 || choice > len(s.People) {
		return nil, fmt.Errorf("choice out of range; run %s stays parked", runID)
	}
	input := workflows.ChoiceInput{Choice: choice}
	if err := submitCLI(ctx, eng, runID, input, args); err != nil {
		return nil, err
	}

	waitCtx2, cancel2 := context.WithTimeout(ctx, time.Minute)
	defer cancel2()
	return eng.Wait(waitCtx2, runID)
}

// Resolve the Hook once per user action. Every transport retry uses this same
// token, ID and payload. A final argument supplies an explicit submission ID.
func submitCLI(ctx context.Context, eng *engine.Engine, runID string, input any, args []string) error {
	run, err := eng.GetRun(ctx, runID)
	if err != nil {
		return err
	}
	var token string
	for _, w := range run.HookWaits {
		if w.Consumed == "" && w.Step == run.Step {
			token = w.Token
		}
	}
	if token == "" {
		return fmt.Errorf("run has no outstanding hook")
	}
	id := uuid.NewString()
	if len(args) > 1 {
		id = args[1]
	}
	payload, err := json.Marshal(input)
	if err != nil {
		return err
	}
	submission := engine.Submission{Token: token, SubmissionID: id, Payload: payload}
	for attempt := 0; attempt < 3; attempt++ {
		receipt, err := eng.Resume(ctx, runID, submission)
		if err == nil {
			fmt.Printf("input accepted at %s (submission %s)\n", receipt.AcceptedAt.Format(time.RFC3339Nano), receipt.SubmissionID)
			return nil
		}
		if errors.Is(err, engine.ErrHookConflict) || errors.Is(err, engine.ErrHookGone) || errors.Is(err, engine.ErrHookValidation) || errors.Is(err, engine.ErrHookCapacity) {
			return err
		}
		if attempt == 2 {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return nil
}
