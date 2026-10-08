package workflows

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"

	"github.com/arekbartnik/nats-workflows/engine"
)

type SwapiState struct {
	People []Person        `json:"people,omitempty"`
	Person json.RawMessage `json:"person,omitempty"`
}

func Swapi() engine.Workflow {
	return engine.Workflow{
		Name: "swapi",
		Steps: []engine.Step{
			{Name: "fetch-people", Run: engine.Typed(fetchPeople)},
			{Name: "random-person", Run: engine.Typed(randomPerson)},
		},
		Result: swapiResult,
	}
}

func SwapiChoose() engine.Workflow {
	return engine.Workflow{
		Name: "swapi-choose",
		Steps: []engine.Step{
			{Name: "fetch-people", Run: engine.Typed(fetchPeople)},
			{Name: "chosen-person", Input: engine.HookType[ChoiceInput]("choice-v1", func(input ChoiceInput) error {
				if input.Choice < 1 {
					return fmt.Errorf("choice must be positive")
				}
				return nil
			}), Run: engine.Typed(chosenPerson)},
		},
		Result: swapiResult,
	}
}

func swapiResult(run *engine.Run) string {
	s, err := engine.StateOf[SwapiState](run)
	if err != nil || len(s.Person) == 0 {
		return ""
	}
	var buf bytes.Buffer
	if json.Indent(&buf, s.Person, "", "  ") != nil {
		return string(s.Person)
	}
	return buf.String()
}

type Person struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

func fetchPeople(ctx context.Context, run *engine.Run, s *SwapiState) error {
	if err := getJSON(ctx, "https://swapi.info/api/people", &s.People); err != nil {
		return err
	}
	if len(s.People) == 0 {
		return errors.New("swapi returned an empty people list")
	}
	return nil
}

func randomPerson(ctx context.Context, run *engine.Run, s *SwapiState) error {
	return fetchPerson(ctx, s.People[rand.IntN(len(s.People))], s)
}

func chosenPerson(ctx context.Context, run *engine.Run, s *SwapiState) error {
	input, err := engine.HookInput[ChoiceInput](ctx, run, "chosen-person", "")
	if err != nil {
		return err
	}
	if input.Choice > len(s.People) {
		return fmt.Errorf("%w: choose a person (1-%d)", engine.ErrTerminal, len(s.People))
	}
	p := s.People[input.Choice-1]
	return fetchPerson(ctx, p, s)
}

func fetchPerson(ctx context.Context, p Person, s *SwapiState) error {
	var detail json.RawMessage
	if err := getJSON(ctx, p.URL, &detail); err != nil {
		return err
	}
	s.Person = detail
	return nil
}

type ChoiceInput struct {
	Choice int `json:"choice"`
}
