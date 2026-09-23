package main

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestExampleApp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out bytes.Buffer
	if err := run(ctx, t.TempDir(), &out); err != nil {
		t.Fatal(err)
	}
	if out.String() != "Hello, Ada!\n" {
		t.Fatalf("output: %q", out.String())
	}
}
