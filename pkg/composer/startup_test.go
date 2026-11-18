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
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/containerd/nerdctl/v2/pkg/healthcheck"
)

func TestWaitForHealthStatusTimeout(t *testing.T) {
	start := time.Now()
	err := waitForHealthStatus(context.Background(), 20*time.Millisecond, func(context.Context) (healthcheck.HealthStatus, bool, error) {
		return healthcheck.Starting, true, nil
	})
	assert.ErrorContains(t, err, "health check timed out")
	assert.Assert(t, time.Since(start) >= 20*time.Millisecond)
}

func TestWaitForHealthStatusHealthy(t *testing.T) {
	err := waitForHealthStatus(context.Background(), time.Second, func(context.Context) (healthcheck.HealthStatus, bool, error) {
		return healthcheck.Healthy, true, nil
	})
	assert.NilError(t, err)
}

func TestRollbackContinuesAndPreservesPreexistingContainers(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "nerdctl.log")
	scriptPath := filepath.Join(t.TempDir(), "nerdctl")
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$ROLLBACK_LOG"
if [ "$1" = "rm" ] && [ "$3" = "fail-id" ]; then
  exit 17
fi
exit 0
`
	assert.NilError(t, os.WriteFile(scriptPath, []byte(script), 0o755))
	t.Setenv("ROLLBACK_LOG", logPath)

	c := &Composer{
		Options: Options{
			NerdctlCmd:    scriptPath,
			NetworkExists: func(string) (bool, error) { return true, nil },
			NetworkInUse:  func(context.Context, string) (bool, error) { return false, nil },
			VolumeExists:  func(string) (bool, error) { return true, nil },
		},
	}
	plan := &dependencyPlan{
		rank:  map[string]int{"old": 0, "failed": 1, "new": 2},
		level: map[string]int{"old": 1, "failed": 2, "new": 3},
	}
	tracker := newStartupTracker(c, plan)
	tracker.recordContainer(startedContainer{id: "old-id", name: "old-1", service: "old", startedByRun: false})
	tracker.recordContainer(startedContainer{id: "fail-id", name: "failed-1", service: "failed", startedByRun: true})
	tracker.recordContainer(startedContainer{id: "new-id", name: "new-1", service: "new", startedByRun: true})
	tracker.resources = []createdResource{
		{kind: "network", name: "project_default"},
		{kind: "volume", name: "project_data"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := tracker.rollback(ctx)
	assert.ErrorContains(t, err, "fail-id")

	data, readErr := os.ReadFile(logPath)
	assert.NilError(t, readErr)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	assert.DeepEqual(t, lines, []string{
		"rm -f new-id",
		"rm -f fail-id",
		"volume rm -f project_data",
		"network rm project_default",
	})
	assert.Assert(t, !strings.Contains(string(data), "old-id"))
}
