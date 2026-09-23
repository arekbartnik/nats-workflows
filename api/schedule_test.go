package api_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/arekbartnik/nats-workflows/engine"
)

func TestSchedulingAPI(t *testing.T) {
	ts := startServer(t)
	base := ts.URL + "/api/v1/runs"
	at := time.Now().Add(30 * 24 * time.Hour).In(time.FixedZone("offset", 7200)).Format(time.RFC3339Nano)
	var run struct {
		ID        string        `json:"id"`
		Status    engine.Status `json:"status"`
		StartAt   time.Time     `json:"start_at"`
		ExpiresAt *time.Time    `json:"expires_at"`
	}
	do(t, http.MethodPost, base, fmt.Sprintf(`{"workflow":"greet","state":{"name":"scheduled"},"start_at":%q}`, at), 201, &run)
	if run.Status != engine.StatusScheduled || run.ExpiresAt != nil || run.StartAt.Location() != time.UTC {
		t.Fatalf("scheduled response: %+v", run)
	}
	url := base + "/" + run.ID
	do(t, http.MethodPost, url+"/reschedule", `{"start_at":"2000-01-01T00:00:00Z"}`, 400, nil)
	do(t, http.MethodPost, url+"/reschedule", `{}`, 400, nil)
	do(t, http.MethodPost, url+"/reschedule", fmt.Sprintf(`{"start_at":%q}`, time.Now().Add(time.Hour).Format(time.RFC3339Nano)), 200, &run)
	do(t, http.MethodPost, url+"/cancel", "", 200, &run)
	if run.Status != engine.StatusCancelled || run.ExpiresAt == nil {
		t.Fatalf("cancelled response: %+v", run)
	}
	do(t, http.MethodGet, url+"/wait?timeout=1s", "", 200, &run)
	if run.Status != engine.StatusCancelled {
		t.Fatal(run.Status)
	}

	do(t, http.MethodPost, url+"/cancel", "", 409, nil)
	do(t, http.MethodPost, url+"/reschedule", fmt.Sprintf(`{"start_at":%q}`, at), 409, nil)
	do(t, http.MethodPost, base+"/missing/cancel", "", 404, nil)

	for _, value := range []string{"2000-01-01T00:00:00Z", "2027-10-01T14:00:00", "invalid"} {
		do(t, http.MethodPost, base, fmt.Sprintf(`{"workflow":"greet","start_at":%q}`, value), 400, nil)
	}
}
