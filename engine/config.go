package engine

import (
	"fmt"
	"slices"
	"strings"
)

type resourceNames struct {
	steps, schedules, runs, hooks, prefix string
}

func namesFor(namespace string) (resourceNames, error) {
	names := resourceNames{steps: "WF_STEPS", schedules: "WF_SCHEDULES", runs: "wf_runs", hooks: "wf_hooks", prefix: "wf"}
	if namespace == "" {
		return names, nil
	}
	if len(namespace) > 64 || strings.ContainsFunc(namespace, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-')
	}) {
		return resourceNames{}, fmt.Errorf("invalid namespace %q: use 1-64 ASCII letters, digits, underscores or hyphens", namespace)
	}
	names.steps += "_" + namespace
	names.schedules += "_" + namespace
	names.runs += "_" + namespace
	names.hooks += "_" + namespace
	// A single subject token keeps named namespaces outside the legacy wf.>
	// subjects and prevents overlap between namespaces such as a and a_b.
	names.prefix += "_" + namespace
	return names, nil
}

func (n resourceNames) subject(suffix string) string { return n.prefix + "." + suffix }

func registeredWorkflows(wfs []Workflow) (map[string]Workflow, error) {
	registered := make(map[string]Workflow, len(wfs))
	for _, wf := range wfs {
		if strings.TrimSpace(wf.Name) == "" {
			return nil, fmt.Errorf("workflow name must not be empty")
		}
		if _, exists := registered[wf.Name]; exists {
			return nil, fmt.Errorf("duplicate workflow %q", wf.Name)
		}
		if len(wf.Steps) == 0 {
			return nil, fmt.Errorf("workflow %q must have at least one step", wf.Name)
		}
		names := make(map[string]bool, len(wf.Steps))
		for i, step := range wf.Steps {
			if strings.TrimSpace(step.Name) == "" || step.Run == nil {
				return nil, fmt.Errorf("workflow %q step %d requires a name and Run function", wf.Name, i)
			}
			if names[step.Name] {
				return nil, fmt.Errorf("workflow %q has duplicate step %q", wf.Name, step.Name)
			}
			names[step.Name] = true
			if step.Input != nil && (step.Input.ID == "" || step.Input.typ == nil || step.Input.validate == nil) {
				return nil, fmt.Errorf("workflow %q step %q requires an input contract created by HookType", wf.Name, step.Name)
			}
		}
		registered[wf.Name] = cloneWorkflow(wf)
	}
	return registered, nil
}

func cloneWorkflow(wf Workflow) Workflow {
	wf.Steps = slices.Clone(wf.Steps)
	for i := range wf.Steps {
		if wf.Steps[i].Input != nil {
			contract := *wf.Steps[i].Input
			wf.Steps[i].Input = &contract
		}
	}
	return wf
}
