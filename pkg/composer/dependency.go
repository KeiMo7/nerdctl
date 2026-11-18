/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package composer

import (
	"fmt"
	"sort"
	"strings"

	"github.com/compose-spec/compose-go/v2/types"
)

type dependencyPlan struct {
	levels       [][]string
	order        []string
	rank         map[string]int
	level        map[string]int
	services     map[string]types.ServiceConfig
	dependencies map[string]map[string]types.ServiceDependency
	dependents   map[string]map[string]struct{}
}

type implicitDependency struct {
	kind     string
	resource string
	services []string
}

func (p *dependencyPlan) String() string {
	levels := make([]string, 0, len(p.levels))
	for i, level := range p.levels {
		levels = append(levels, fmt.Sprintf("L%d=[%s]", i+1, strings.Join(level, ",")))
	}
	return strings.Join(levels, " ")
}

func (p *dependencyPlan) dependentConditions(name string) []string {
	var conditions []string
	for dependent := range p.dependents[name] {
		for dep, cfg := range p.dependencies[dependent] {
			if dep != name {
				continue
			}
			condition := cfg.Condition
			if condition == "" {
				condition = types.ServiceConditionStarted
			}
			conditions = append(conditions, condition)
		}
	}
	sort.Strings(conditions)
	return conditions
}

// newDependencyPlan builds a deterministic dependency plan for the selected
// services. Selection follows Compose service-name and wildcard semantics.
// When noDeps is false, required and optional dependencies of the selected
// services are included.
func newDependencyPlan(project *types.Project, selectedServices []string, noDeps bool) (*dependencyPlan, error) {
	roots := make(map[string]struct{})
	err := project.ForEachService(selectedServices, func(name string, _ *types.ServiceConfig) error {
		roots[name] = struct{}{}
		return nil
	}, types.IgnoreDependencies)
	if err != nil {
		return nil, err
	}

	services := make(map[string]types.ServiceConfig, len(roots))
	seen := make(map[string]bool, len(roots))
	var addService func(name string) error
	addService = func(name string) error {
		if seen[name] {
			return nil
		}
		seen[name] = true

		svc, ok := project.Services[name]
		if !ok {
			svc, ok = project.DisabledServices[name]
		}
		if !ok {
			return fmt.Errorf("no such service: %s", name)
		}
		services[name] = svc

		if noDeps {
			return nil
		}
		for depName, dep := range svc.DependsOn {
			if _, ok := project.Services[depName]; !ok {
				if _, disabled := project.DisabledServices[depName]; !disabled {
					if dep.Required {
						return fmt.Errorf("service %q depends on undefined service %q", name, depName)
					}
					continue
				}
			}
			if err := addService(depName); err != nil {
				return err
			}
		}
		if target, ok := serviceNetworkModeTarget(svc); ok {
			if _, exists := project.Services[target]; exists {
				if err := addService(target); err != nil {
					return err
				}
			}
		}
		for _, volumesFrom := range svc.VolumesFrom {
			target := strings.SplitN(volumesFrom, ":", 2)[0]
			if _, exists := project.Services[target]; exists {
				if err := addService(target); err != nil {
					return err
				}
			}
		}
		return nil
	}

	rootNames := sortedKeys(roots)
	for _, name := range rootNames {
		if err := addService(name); err != nil {
			return nil, err
		}
	}

	deps := make(map[string]map[string]types.ServiceDependency, len(services))
	dependents := make(map[string]map[string]struct{}, len(services))
	for name := range services {
		deps[name] = make(map[string]types.ServiceDependency)
	}

	for name, svc := range services {
		if !noDeps {
			for depName, dep := range svc.DependsOn {
				if _, ok := services[depName]; !ok {
					if dep.Required {
						return nil, fmt.Errorf("service %q depends on undefined service %q", name, depName)
					}
					continue
				}
				deps[name][depName] = dep
			}
		}

		if target, ok := serviceNetworkModeTarget(svc); ok {
			if _, selected := services[target]; selected {
				deps[name][target] = types.ServiceDependency{Condition: types.ServiceConditionStarted, Required: true}
			}
		}

		for _, volumesFrom := range svc.VolumesFrom {
			target := strings.SplitN(volumesFrom, ":", 2)[0]
			if _, selected := services[target]; selected {
				deps[name][target] = types.ServiceDependency{Condition: types.ServiceConditionStarted, Required: true}
			}
		}
	}

	for name, serviceDeps := range deps {
		for dep := range serviceDeps {
			if dependents[dep] == nil {
				dependents[dep] = make(map[string]struct{})
			}
			dependents[dep][name] = struct{}{}
		}
	}

	levels, order, err := dependencyLevels(services, deps)
	if err != nil {
		return nil, err
	}
	rank := make(map[string]int, len(order))
	for i, name := range order {
		rank[name] = i
	}
	levelByService := make(map[string]int, len(order))
	for i, level := range levels {
		for _, name := range level {
			levelByService[name] = i + 1
		}
	}

	return &dependencyPlan{
		levels:       levels,
		order:        order,
		rank:         rank,
		level:        levelByService,
		services:     services,
		dependencies: deps,
		dependents:   dependents,
	}, nil
}

func dependencyLevels(services map[string]types.ServiceConfig, deps map[string]map[string]types.ServiceDependency) ([][]string, []string, error) {
	indegree := make(map[string]int, len(services))
	children := make(map[string][]string, len(services))
	for name := range services {
		indegree[name] = len(deps[name])
	}
	for name, serviceDeps := range deps {
		for dep := range serviceDeps {
			children[dep] = append(children[dep], name)
		}
	}
	for name := range children {
		sort.Strings(children[name])
	}

	remaining := make(map[string]struct{}, len(services))
	var ready []string
	for name, degree := range indegree {
		remaining[name] = struct{}{}
		if degree == 0 {
			ready = append(ready, name)
		}
	}
	sort.Strings(ready)

	var levels [][]string
	var order []string
	for len(ready) > 0 {
		level := append([]string(nil), ready...)
		levels = append(levels, level)
		order = append(order, level...)

		next := make([]string, 0)
		for _, name := range level {
			delete(remaining, name)
			for _, child := range children[name] {
				indegree[child]--
				if indegree[child] == 0 {
					next = append(next, child)
				}
			}
		}
		sort.Strings(next)
		ready = next
	}

	if len(remaining) > 0 {
		cycle := dependencyCycle(remaining, deps)
		return nil, nil, fmt.Errorf("dependency cycle detected: %s", strings.Join(cycle, " -> "))
	}
	return levels, order, nil
}

func dependencyCycle(remaining map[string]struct{}, deps map[string]map[string]types.ServiceDependency) []string {
	const (
		unvisited = iota
		visiting
		visited
	)
	state := make(map[string]int, len(remaining))
	var stack []string

	var visit func(string) []string
	visit = func(name string) []string {
		state[name] = visiting
		stack = append(stack, name)

		for _, dep := range sortedKeys(deps[name]) {
			if _, ok := remaining[dep]; !ok {
				continue
			}
			switch state[dep] {
			case unvisited:
				if cycle := visit(dep); cycle != nil {
					return cycle
				}
			case visiting:
				for i, item := range stack {
					if item == dep {
						return append(append([]string(nil), stack[i:]...), dep)
					}
				}
			}
		}

		stack = stack[:len(stack)-1]
		state[name] = visited
		return nil
	}

	for _, name := range sortedKeys(remaining) {
		if state[name] == unvisited {
			if cycle := visit(name); cycle != nil {
				return cycle
			}
		}
	}
	return nil
}

func implicitDependencies(project *types.Project, plan *dependencyPlan) []implicitDependency {
	networks := make(map[string][]string)
	volumes := make(map[string][]string)

	for name, svc := range plan.services {
		for shortName := range svc.Networks {
			resource := shortName
			if net, ok := project.Networks[shortName]; ok && net.Name != "" {
				resource = net.Name
			}
			networks[resource] = append(networks[resource], name)
		}
		for _, volume := range svc.Volumes {
			if volume.Type != types.VolumeTypeVolume || volume.Source == "" {
				continue
			}
			volumes[volume.Source] = append(volumes[volume.Source], name)
		}
	}

	var warnings []implicitDependency
	for resource, services := range networks {
		warnings = append(warnings, newImplicitDependency("network", resource, services))
	}
	for resource, services := range volumes {
		warnings = append(warnings, newImplicitDependency("volume", resource, services))
	}
	sort.Slice(warnings, func(i, j int) bool {
		if warnings[i].kind != warnings[j].kind {
			return warnings[i].kind < warnings[j].kind
		}
		return warnings[i].resource < warnings[j].resource
	})

	filtered := warnings[:0]
	for _, warning := range warnings {
		if len(warning.services) > 1 {
			filtered = append(filtered, warning)
		}
	}
	return filtered
}

func newImplicitDependency(kind, resource string, services []string) implicitDependency {
	services = append([]string(nil), services...)
	sort.Strings(services)
	return implicitDependency{kind: kind, resource: resource, services: services}
}

func (d implicitDependency) String() string {
	return fmt.Sprintf("implicit dependency via shared %s %q between services [%s]; no depends_on edge is inferred",
		d.kind, d.resource, strings.Join(d.services, ", "))
}

func serviceNetworkModeTarget(svc types.ServiceConfig) (string, bool) {
	if !strings.HasPrefix(svc.NetworkMode, types.ServicePrefix) {
		return "", false
	}
	return strings.TrimPrefix(svc.NetworkMode, types.ServicePrefix), true
}

func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
