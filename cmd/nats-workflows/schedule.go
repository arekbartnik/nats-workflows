package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/arekbartnik/nats-workflows/engine"
	"github.com/arekbartnik/nats-workflows/examples/workflows"
)

func scheduledCommand(ctx context.Context, eng *engine.Engine, cmd string, args []string) (bool, error) {
	if cmd == "cancel" {
		if len(args) != 1 {
			return true, fmt.Errorf("usage: cancel <runID>")
		}
		return true, eng.Cancel(ctx, args[0])
	}
	if cmd == "reschedule" {
		if len(args) != 2 {
			return true, fmt.Errorf("usage: reschedule <runID> <RFC3339 timestamp>")
		}
		at, err := time.Parse(time.RFC3339Nano, args[1])
		if err != nil {
			return true, err
		}
		return true, eng.Reschedule(ctx, args[0], at)
	}
	var atText string
	var positional []string
	found := false
	for i := 0; i < len(args); i++ {
		if args[i] == "--at" || strings.HasPrefix(args[i], "--at=") {
			if found {
				return true, fmt.Errorf("--at may only be supplied once")
			}
			found = true
			if value, ok := strings.CutPrefix(args[i], "--at="); ok {
				atText = value
			} else {
				i++
				if i == len(args) {
					return true, fmt.Errorf("--at requires an RFC3339 timestamp")
				}
				atText = args[i]
			}
		} else {
			positional = append(positional, args[i])
		}
	}
	if !found {
		return false, nil
	}
	at, err := time.Parse(time.RFC3339Nano, atText)
	if err != nil {
		return true, err
	}
	var workflow string
	var initial any
	switch cmd {
	case "random", "choose":
		if len(positional) != 0 {
			return true, fmt.Errorf("%s --at starts a new run and takes no positional arguments", cmd)
		}
		workflow = "swapi"
		if cmd == "choose" {
			workflow = "swapi-choose"
		}
	case "ask":
		question := strings.TrimSpace(strings.Join(positional, " "))
		if question == "" {
			return true, fmt.Errorf("ask --at requires a question")
		}
		workflow, initial = "ask", workflows.AskState{Question: question}
	case "order":
		if len(positional) > 1 {
			return true, fmt.Errorf("usage: order [amount] --at <timestamp>")
		}
		amount := 250
		if len(positional) == 1 {
			amount, err = strconv.Atoi(positional[0])
			if err != nil {
				return true, err
			}
		}
		workflow, initial = "order", workflows.OrderState{Order: workflows.Order{Item: "starship model kit", Qty: 1, Amount: amount}}
	default:
		return true, fmt.Errorf("--at is only available for random, choose, ask, and order")
	}
	id, err := eng.Schedule(ctx, workflow, initial, at)
	if err != nil {
		return true, err
	}
	fmt.Printf("run %s scheduled for %s\n", id, at.UTC().Format(time.RFC3339Nano))
	return true, nil
}
