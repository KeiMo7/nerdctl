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
	"strings"
	"testing"

	"github.com/compose-spec/compose-go/v2/types"
	"gotest.tools/v3/assert"
)

func TestDependencyPlanChain(t *testing.T) {
	project := testDependencyProject(map[string]types.ServiceConfig{
		"app":   serviceDependingOn("app", "db"),
		"db":    serviceDependingOn("db", "cache"),
		"cache": testService("cache"),
	})

	plan, err := newDependencyPlan(project, nil, false)
	assert.NilError(t, err)
	assert.DeepEqual(t, plan.levels, [][]string{{"cache"}, {"db"}, {"app"}})
	assert.DeepEqual(t, plan.order, []string{"cache", "db", "app"})
	assert.Equal(t, plan.level["cache"], 1)
	assert.Equal(t, plan.level["db"], 2)
	assert.Equal(t, plan.level["app"], 3)
}

func TestDependencyPlanParallelLevelsAreDeterministic(t *testing.T) {
	project := testDependencyProject(map[string]types.ServiceConfig{
		"web":   serviceDependingOn("web", "db", "cache"),
		"db":    testService("db"),
		"cache": testService("cache"),
	})

	for i := 0; i < 20; i++ {
		plan, err := newDependencyPlan(project, nil, false)
		assert.NilError(t, err)
		assert.DeepEqual(t, plan.levels, [][]string{{"cache", "db"}, {"web"}})
	}
}

func TestDependencyPlanCycle(t *testing.T) {
	project := testDependencyProject(map[string]types.ServiceConfig{
		"a": serviceDependingOn("a", "b"),
		"b": serviceDependingOn("b", "c"),
		"c": serviceDependingOn("c", "a"),
	})

	_, err := newDependencyPlan(project, nil, false)
	assert.ErrorContains(t, err, "dependency cycle detected")
	assert.ErrorContains(t, err, "a -> b -> c -> a")
}

func TestDependencyPlanMissingDependency(t *testing.T) {
	project := testDependencyProject(map[string]types.ServiceConfig{
		"app": serviceDependingOn("app", "missing"),
	})

	_, err := newDependencyPlan(project, nil, false)
	assert.ErrorContains(t, err, `service "app" depends on undefined service "missing"`)
}

func TestDependencyPlanSelectedSubgraph(t *testing.T) {
	project := testDependencyProject(map[string]types.ServiceConfig{
		"app":     serviceDependingOn("app", "db"),
		"db":      serviceDependingOn("db", "cache"),
		"cache":   testService("cache"),
		"worker":  serviceDependingOn("worker", "queue"),
		"queue":   testService("queue"),
		"metrics": testService("metrics"),
	})

	plan, err := newDependencyPlan(project, []string{"app"}, false)
	assert.NilError(t, err)
	assert.DeepEqual(t, plan.order, []string{"cache", "db", "app"})

	noDepsPlan, err := newDependencyPlan(project, []string{"app"}, true)
	assert.NilError(t, err)
	assert.DeepEqual(t, noDepsPlan.order, []string{"app"})
}

func TestImplicitDependencies(t *testing.T) {
	project := testDependencyProject(map[string]types.ServiceConfig{
		"app": {
			Name: "app",
			ContainerSpec: types.ContainerSpec{
				Networks: map[string]*types.ServiceNetworkConfig{
					"shared": nil,
				},
				Volumes: []types.ServiceVolumeConfig{
					{Type: types.VolumeTypeVolume, Source: "data"},
				},
			},
		},
		"db": {
			Name: "db",
			ContainerSpec: types.ContainerSpec{
				Networks: map[string]*types.ServiceNetworkConfig{
					"shared": nil,
				},
				Volumes: []types.ServiceVolumeConfig{
					{Type: types.VolumeTypeVolume, Source: "data"},
				},
			},
		},
	})
	project.Networks = map[string]types.NetworkConfig{
		"shared": {Name: "project_shared"},
	}

	plan, err := newDependencyPlan(project, nil, false)
	assert.NilError(t, err)
	warnings := implicitDependencies(project, plan)
	assert.Equal(t, len(warnings), 2)
	assert.Assert(t, strings.Contains(warnings[0].String(), "shared network"))
	assert.Assert(t, strings.Contains(warnings[1].String(), "shared volume"))
}

func TestDependencyPlanNetworkModeTarget(t *testing.T) {
	project := testDependencyProject(map[string]types.ServiceConfig{
		"app": {Name: "app", ContainerSpec: types.ContainerSpec{NetworkMode: "service:proxy"}},
		"proxy": {
			Name: "proxy",
		},
	})

	plan, err := newDependencyPlan(project, []string{"app"}, false)
	assert.NilError(t, err)
	assert.DeepEqual(t, plan.order, []string{"proxy", "app"})
}

func testDependencyProject(services map[string]types.ServiceConfig) *types.Project {
	return &types.Project{
		Name:     "test",
		Services: types.Services(services),
	}
}

func testService(name string) types.ServiceConfig {
	return types.ServiceConfig{Name: name}
}

func serviceDependingOn(name string, dependencies ...string) types.ServiceConfig {
	service := testService(name)
	service.DependsOn = make(types.DependsOnConfig, len(dependencies))
	for _, dependency := range dependencies {
		service.DependsOn[dependency] = types.ServiceDependency{
			Condition: types.ServiceConditionStarted,
			Required:  true,
		}
	}
	return service
}
