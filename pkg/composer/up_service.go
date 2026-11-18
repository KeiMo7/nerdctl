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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/compose-spec/compose-go/v2/types"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/errdefs"
	"golang.org/x/sync/errgroup"

	"github.com/containerd/log"

	"github.com/containerd/nerdctl/v2/pkg/composer/serviceparser"
	"github.com/containerd/nerdctl/v2/pkg/healthcheck"
	"github.com/containerd/nerdctl/v2/pkg/internal/filesystem"
	"github.com/containerd/nerdctl/v2/pkg/labels"
)

func (c *Composer) upServices(ctx context.Context, plan *dependencyPlan, parsedServices []*serviceparser.Service, uo UpOptions) error {
	if len(parsedServices) == 0 {
		return errors.New("no service was provided")
	}

	// TODO: parallelize loop for ensuring images (make sure not to mess up tty)
	for _, ps := range parsedServices {
		if err := c.ensureServiceImage(ctx, ps, !uo.NoBuild, uo.ForceBuild, BuildOptions{}, uo.QuietPull, uo.Pull); err != nil {
			return err
		}
	}

	recreate := uo.recreateStrategy()

	servicesByName := make(map[string]*serviceparser.Service, len(parsedServices))
	services := make([]string, 0, len(parsedServices))
	for _, ps := range parsedServices {
		servicesByName[ps.Unparsed.Name] = ps
		services = append(services, ps.Unparsed.Name)
	}

	tracker := newStartupTracker(c, plan)
	log.G(ctx).Infof("[compose-plan] time=%s order=%s", eventTime(), plan.String())
	if err := tracker.ensurePlanResources(ctx); err != nil {
		rollbackErr := tracker.rollback(ctx)
		if rollbackErr != nil {
			return errors.Join(err, rollbackErr)
		}
		return err
	}
	if err := c.startPlan(ctx, plan, servicesByName, tracker, recreate); err != nil {
		rollbackErr := tracker.rollback(ctx)
		if rollbackErr != nil {
			return errors.Join(err, rollbackErr)
		}
		return err
	}

	if uo.Detach {
		return nil
	}

	containers := tracker.containersByID()
	// this is used to stop containers in case --abort-on-container-exit flag is set.
	// c.Logs returns an error, so we don't need Ctrl-c to reach the "Stopping containers (forcibly)"
	if uo.AbortOnContainerExit {
		defer c.stopContainersFromParsedServices(ctx, containers)
	}
	log.G(ctx).Info("Attaching to logs")
	lo := LogsOptions{
		AbortOnContainerExit: uo.AbortOnContainerExit,
		Follow:               true,
		NoColor:              uo.NoColor,
		NoLogPrefix:          uo.NoLogPrefix,
		LatestRun:            recreate == RecreateNever,
	}
	if err := c.Logs(ctx, lo, services); err != nil {
		rollbackErr := tracker.rollback(ctx)
		if rollbackErr != nil {
			return errors.Join(err, rollbackErr)
		}
		return err
	}

	log.G(ctx).Infof("Stopping containers (forcibly)") // TODO: support gracefully stopping
	c.stopContainersFromParsedServices(ctx, containers)
	return nil
}

func (c *Composer) startPlan(ctx context.Context, plan *dependencyPlan, services map[string]*serviceparser.Service, tracker *startupTracker, recreate string) error {
	for levelIndex, level := range plan.levels {
		log.G(ctx).Infof("[compose-startup] time=%s level=%d event=start services=[%s]",
			eventTime(), levelIndex+1, strings.Join(level, ", "))

		group, levelCtx := errgroup.WithContext(ctx)
		for _, name := range level {
			name := name
			group.Go(func() error {
				ps, ok := services[name]
				if !ok {
					return fmt.Errorf("service %q was selected but not parsed", name)
				}
				if err := tracker.startService(levelCtx, ps, recreate); err != nil {
					return fmt.Errorf("service %q: %w", name, err)
				}
				return nil
			})
		}
		if err := group.Wait(); err != nil {
			return fmt.Errorf("startup failed in level %d: %w", levelIndex+1, err)
		}
	}
	return nil
}

type startedContainer struct {
	id           string
	name         string
	service      string
	startedByRun bool
}

type createdResource struct {
	kind string
	name string
}

type startupTracker struct {
	c *Composer

	plan         *dependencyPlan
	containersMu sync.Mutex
	containers   map[string][]startedContainer
	resourcesMu  sync.Mutex
	resources    []createdResource
	resourceDone map[string]bool
}

func newStartupTracker(c *Composer, plan *dependencyPlan) *startupTracker {
	return &startupTracker{
		c:            c,
		plan:         plan,
		containers:   make(map[string][]startedContainer),
		resourceDone: make(map[string]bool),
	}
}

func (t *startupTracker) startService(ctx context.Context, service *serviceparser.Service, recreate string) error {
	name := service.Unparsed.Name
	level := t.plan.level[name]
	log.G(ctx).Infof("[compose-startup] time=%s level=%d service=%q event=starting", eventTime(), level, name)

	if err := t.ensureServiceResources(ctx, service); err != nil {
		return err
	}

	var (
		containerIDs []string
		idsMu        sync.Mutex
	)
	group, runCtx := errgroup.WithContext(ctx)
	for _, container := range service.Containers {
		container := container
		group.Go(func() error {
			existingID, err := t.c.containerID(runCtx, container.Name, name)
			if err != nil {
				return fmt.Errorf("checking container %q: %w", container.Name, err)
			}
			wasRunning := false
			if existingID != "" {
				wasRunning, err = t.c.containerIsRunning(runCtx, existingID)
				if err != nil {
					return fmt.Errorf("checking whether container %q is running: %w", container.Name, err)
				}
			}

			id, err := t.c.upServiceContainer(runCtx, service, container, recreate)
			if err != nil {
				// A failed run may still have created a container. Track it so
				// rollback cleans up this run without touching older containers.
				if currentID, lookupErr := t.c.containerID(context.WithoutCancel(ctx), container.Name, name); lookupErr == nil &&
					currentID != "" && currentID != existingID {
					t.recordContainer(startedContainer{id: currentID, name: container.Name, service: name, startedByRun: true})
				}
				return fmt.Errorf("starting container %q: %w", container.Name, err)
			}
			if id == "" {
				return fmt.Errorf("container %q returned an empty ID", container.Name)
			}

			t.recordContainer(startedContainer{id: id, name: container.Name, service: name, startedByRun: !wasRunning})
			idsMu.Lock()
			containerIDs = append(containerIDs, id)
			idsMu.Unlock()
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}

	log.G(ctx).Infof("[compose-startup] time=%s level=%d service=%q event=started", eventTime(), level, name)
	if len(t.plan.dependents[name]) > 0 {
		if err := t.waitForServiceReadiness(ctx, service, containerIDs); err != nil {
			return err
		}
	}
	log.G(ctx).Infof("[compose-startup] time=%s level=%d service=%q event=ready", eventTime(), level, name)
	return nil
}

func (t *startupTracker) ensurePlanResources(ctx context.Context) error {
	for _, name := range t.plan.order {
		service := t.plan.services[name]
		parsed := &serviceparser.Service{Unparsed: &service}
		if err := t.ensureServiceResources(ctx, parsed); err != nil {
			return fmt.Errorf("service %q: %w", name, err)
		}
	}
	return nil
}

func (t *startupTracker) ensureServiceResources(ctx context.Context, service *serviceparser.Service) error {
	t.resourcesMu.Lock()
	defer t.resourcesMu.Unlock()

	networkNames := sortedKeys(service.Unparsed.Networks)
	for _, shortName := range networkNames {
		key := "network/" + shortName
		if t.resourceDone[key] {
			continue
		}
		created, err := t.c.upNetwork(ctx, shortName)
		if err != nil {
			return fmt.Errorf("preparing network %q: %w", shortName, err)
		}
		if created {
			net := t.c.project.Networks[shortName]
			t.resources = append(t.resources, createdResource{kind: "network", name: net.Name})
			log.G(ctx).Infof("[compose-resource] time=%s action=create kind=network name=%q service=%q",
				eventTime(), net.Name, service.Unparsed.Name)
		}
		t.resourceDone[key] = true
	}

	volumeNames := make([]string, 0)
	for _, volume := range service.Unparsed.Volumes {
		if volume.Type == "volume" && volume.Source != "" {
			volumeNames = append(volumeNames, volume.Source)
		}
	}
	sort.Strings(volumeNames)
	for _, shortName := range volumeNames {
		key := "volume/" + shortName
		if t.resourceDone[key] {
			continue
		}
		created, err := t.c.upVolume(ctx, shortName)
		if err != nil {
			return fmt.Errorf("preparing volume %q: %w", shortName, err)
		}
		if created {
			vol := t.c.project.Volumes[shortName]
			t.resources = append(t.resources, createdResource{kind: "volume", name: vol.Name})
			log.G(ctx).Infof("[compose-resource] time=%s action=create kind=volume name=%q service=%q",
				eventTime(), vol.Name, service.Unparsed.Name)
		}
		t.resourceDone[key] = true
	}
	return nil
}

func (t *startupTracker) recordContainer(container startedContainer) {
	t.containersMu.Lock()
	defer t.containersMu.Unlock()
	for _, existing := range t.containers[container.service] {
		if existing.id == container.id {
			return
		}
	}
	t.containers[container.service] = append(t.containers[container.service], container)
}

func (t *startupTracker) containersByID() map[string]serviceparser.Container {
	t.containersMu.Lock()
	defer t.containersMu.Unlock()

	containers := make(map[string]serviceparser.Container)
	for _, records := range t.containers {
		for _, record := range records {
			containers[record.id] = serviceparser.Container{Name: record.name}
		}
	}
	return containers
}

func (t *startupTracker) waitForServiceReadiness(ctx context.Context, service *serviceparser.Service, containerIDs []string) error {
	conditions := t.plan.dependentConditions(service.Unparsed.Name)
	requiresCompletion := false
	requiresHealthy := false
	for _, condition := range conditions {
		switch condition {
		case types.ServiceConditionCompletedSuccessfully:
			requiresCompletion = true
		case types.ServiceConditionHealthy:
			requiresHealthy = true
		}
	}

	for _, id := range containerIDs {
		con, err := t.c.client.LoadContainer(ctx, id)
		if err != nil {
			return fmt.Errorf("loading container %s for readiness: %w", id, err)
		}
		if requiresCompletion {
			if err := t.c.waitForContainerCompletion(ctx, con); err != nil {
				return err
			}
			continue
		}

		hc, err := containerHealthcheck(ctx, con)
		if err != nil {
			return err
		}
		if hc == nil || len(hc.Test) == 0 || hc.Test[0] == healthcheck.CmdNone {
			if requiresHealthy {
				return fmt.Errorf("dependency %q requested service_healthy but has no healthcheck", service.Unparsed.Name)
			}
			continue
		}

		// A configured healthcheck always gates dependents, including for the
		// default service_started condition.
		if err := t.c.waitForContainerHealthy(ctx, con, hc); err != nil {
			return err
		}
	}
	return nil
}

func containerHealthcheck(ctx context.Context, container containerd.Container) (*healthcheck.Healthcheck, error) {
	info, err := container.Info(ctx)
	if err != nil {
		return nil, err
	}
	value := info.Labels[labels.HealthCheck]
	if value == "" {
		return nil, nil
	}
	hc, err := healthcheck.HealthCheckFromJSON(value)
	if err != nil {
		return nil, fmt.Errorf("parsing healthcheck: %w", err)
	}
	return hc, nil
}

func (t *startupTracker) rollback(ctx context.Context) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()

	t.containersMu.Lock()
	recordsByService := make(map[string][]startedContainer, len(t.containers))
	for service, records := range t.containers {
		recordsByService[service] = append([]startedContainer(nil), records...)
	}
	t.containersMu.Unlock()

	serviceNames := sortedKeys(recordsByService)
	sort.Slice(serviceNames, func(i, j int) bool {
		left, right := t.plan.rank[serviceNames[i]], t.plan.rank[serviceNames[j]]
		if left == right {
			return serviceNames[i] > serviceNames[j]
		}
		return left > right
	})

	var errs []error
	var rolledBack []string
	var failed []string
	for _, service := range serviceNames {
		records := recordsByService[service]
		sort.Slice(records, func(i, j int) bool {
			return records[i].name < records[j].name
		})
		rolledService := false
		for _, record := range records {
			if !record.startedByRun {
				continue
			}
			log.G(cleanupCtx).Infof("[compose-rollback] time=%s action=remove-container service=%q container=%q id=%s",
				eventTime(), service, record.name, record.id)
			if err := t.c.runNerdctlCmd(cleanupCtx, "rm", "-f", record.id); err != nil {
				wrapped := fmt.Errorf("rollback service %q container %q: %w", service, record.name, err)
				errs = append(errs, wrapped)
				failed = append(failed, fmt.Sprintf("container:%s", record.name))
				log.G(cleanupCtx).Warn(wrapped)
				continue
			}
			rolledService = true
		}
		if rolledService {
			rolledBack = append(rolledBack, service)
		}
	}

	t.resourcesMu.Lock()
	resources := append([]createdResource(nil), t.resources...)
	t.resourcesMu.Unlock()
	for i := len(resources) - 1; i >= 0; i-- {
		resource := resources[i]
		log.G(cleanupCtx).Infof("[compose-rollback] time=%s action=remove-%s name=%q",
			eventTime(), resource.kind, resource.name)

		var err error
		switch resource.kind {
		case "network":
			err = t.c.removeNetworkByName(cleanupCtx, resource.name)
		case "volume":
			err = t.c.removeVolumeByName(cleanupCtx, resource.name)
		default:
			err = fmt.Errorf("unknown resource kind %q", resource.kind)
		}
		if err != nil {
			wrapped := fmt.Errorf("rollback %s %q: %w", resource.kind, resource.name, err)
			errs = append(errs, wrapped)
			failed = append(failed, fmt.Sprintf("%s:%s", resource.kind, resource.name))
			log.G(cleanupCtx).Warn(wrapped)
			continue
		}
		rolledBack = append(rolledBack, fmt.Sprintf("%s:%s", resource.kind, resource.name))
	}

	log.G(cleanupCtx).Infof("[compose-rollback] time=%s action=summary rolled-back=[%s] failed=[%s]",
		eventTime(), strings.Join(rolledBack, ", "), strings.Join(failed, ", "))
	return errors.Join(errs...)
}

func (c *Composer) removeNetworkByName(ctx context.Context, fullName string) error {
	exists, err := c.NetworkExists(fullName)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	used, err := c.NetworkInUse(ctx, fullName)
	if err != nil {
		return err
	}
	if used {
		return fmt.Errorf("network is in use")
	}
	return c.runNerdctlCmd(ctx, "network", "rm", fullName)
}

func (c *Composer) removeVolumeByName(ctx context.Context, fullName string) error {
	exists, err := c.VolumeExists(fullName)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	return c.runNerdctlCmd(ctx, "volume", "rm", "-f", fullName)
}

func (c *Composer) containerIsRunning(ctx context.Context, id string) (bool, error) {
	container, err := c.client.LoadContainer(ctx, id)
	if err != nil {
		return false, err
	}
	task, err := container.Task(ctx, nil)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	status, err := task.Status(ctx)
	if err != nil {
		return false, err
	}
	return status.Status != containerd.Stopped, nil
}

func (c *Composer) waitForContainerHealthy(ctx context.Context, container containerd.Container, hc *healthcheck.Healthcheck) error {
	hc.ApplyDefaults()
	timeout := healthcheckTimeout(hc)
	return waitForHealthStatus(ctx, timeout, func(ctx context.Context) (healthcheck.HealthStatus, bool, error) {
		task, err := container.Task(ctx, nil)
		if err != nil {
			return "", false, fmt.Errorf("health check failed: container task is unavailable: %w", err)
		}
		status, err := task.Status(ctx)
		if err != nil {
			return "", false, fmt.Errorf("health check failed: reading container status: %w", err)
		}

		info, err := container.Info(ctx)
		if err != nil {
			return "", false, err
		}
		healthStatus := healthcheck.Starting
		if stateJSON := info.Labels[labels.HealthState]; stateJSON != "" {
			state, err := healthcheck.HealthStateFromJSON(stateJSON)
			if err != nil {
				return "", false, fmt.Errorf("parsing health state: %w", err)
			}
			healthStatus = state.Status
		}
		return healthStatus, status.Status != containerd.Stopped, nil
	})
}

func waitForHealthStatus(ctx context.Context, timeout time.Duration, read func(context.Context) (healthcheck.HealthStatus, bool, error)) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	lastStatus := "starting"
	for {
		status, running, err := read(ctx)
		if err != nil {
			return err
		}
		if !running {
			return fmt.Errorf("health check failed: container stopped before becoming healthy")
		}
		lastStatus = status
		if status == healthcheck.Healthy {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return fmt.Errorf("health check timed out after %s (last status: %s)", timeout, lastStatus)
		case <-ticker.C:
		}
	}
}

func (c *Composer) waitForContainerCompletion(ctx context.Context, container containerd.Container) error {
	task, err := container.Task(ctx, nil)
	if err != nil {
		return fmt.Errorf("waiting for completion: %w", err)
	}
	statusC, err := task.Wait(ctx)
	if err != nil {
		return fmt.Errorf("waiting for completion: %w", err)
	}

	timer := time.NewTimer(defaultReadinessTimeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("completion timeout after %s", defaultReadinessTimeout)
	case status := <-statusC:
		code, _, err := status.Result()
		if err != nil {
			return fmt.Errorf("reading completion status: %w", err)
		}
		if code != 0 {
			return fmt.Errorf("exited with status %d", code)
		}
		return nil
	}
}

const defaultReadinessTimeout = 2 * time.Minute

func healthcheckTimeout(hc *healthcheck.Healthcheck) time.Duration {
	timeout := hc.StartPeriod + hc.Interval*time.Duration(hc.Retries+1) + hc.Timeout + 2*time.Second
	if timeout < 5*time.Second {
		return 5 * time.Second
	}
	return timeout
}

func eventTime() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

func (c *Composer) ensureServiceImage(ctx context.Context, ps *serviceparser.Service, allowBuild, forceBuild bool, bo BuildOptions, quiet bool, pullModeArg string) error {
	pullMode := ps.PullMode
	if pullModeArg != "" {
		pullMode = pullModeArg
	}

	if ps.Build != nil && allowBuild {
		if ps.Build.Force || forceBuild {
			if err := c.buildServiceImage(ctx, ps.Image, ps.Build, ps.Unparsed.Platform, bo); err != nil {
				return err
			}
			return c.ensureImageMountSources(ctx, ps, pullMode, quiet)
		}
		if ok, err := c.ImageExists(ctx, ps.Image); err != nil {
			return err
		} else if !ok {
			if err := c.buildServiceImage(ctx, ps.Image, ps.Build, ps.Unparsed.Platform, bo); err != nil {
				return err
			}
			return c.ensureImageMountSources(ctx, ps, pullMode, quiet)
		}
		// even when c.ImageExists returns true, we need to call c.EnsureImage
		// because ps.PullMode can be "always". So no return here.
		log.G(ctx).Debugf("Image %s already exists, not building", ps.Image)
	}

	log.G(ctx).Infof("Ensuring image %s", ps.Image)
	if err := c.EnsureImage(ctx, ps.Image, pullMode, ps.Unparsed.Platform, ps, quiet); err != nil {
		return err
	}
	return c.ensureImageMountSources(ctx, ps, pullMode, quiet)
}

func (c *Composer) ensureImageMountSources(ctx context.Context, ps *serviceparser.Service, pullMode string, quiet bool) error {
	seen := make(map[serviceparser.ImageMountSource]struct{})
	for _, source := range ps.ImageMountSources {
		if _, ok := seen[source]; ok {
			continue
		}
		seen[source] = struct{}{}

		log.G(ctx).Infof("Ensuring image mount source %s", source.Source)
		if err := c.EnsureImage(ctx, source.Source, pullMode, source.Platform, ps, quiet); err != nil {
			return fmt.Errorf("failed to ensure image %q for image volume: %w", source.Source, err)
		}
	}
	return nil
}

// upServiceContainer must be called after ensureServiceImage
// upServiceContainer returns container ID
func (c *Composer) upServiceContainer(ctx context.Context, service *serviceparser.Service, container serviceparser.Container, recreate string) (string, error) {
	// check if container already exists
	existingCid, err := c.containerID(ctx, container.Name, service.Unparsed.Name)
	if err != nil {
		return "", fmt.Errorf("error while checking for containers with name %q: %w", container.Name, err)
	}

	// FIXME
	if service.Unparsed.StdinOpen != service.Unparsed.Tty {
		return "", fmt.Errorf("currently StdinOpen(-i) and Tty(-t) should be same")
	}

	var runFlagD bool
	if !service.Unparsed.StdinOpen && !service.Unparsed.Tty {
		container.RunArgs = append([]string{"-d"}, container.RunArgs...)
		runFlagD = true
	}

	// start the existing container and exit early
	if existingCid != "" && recreate == RecreateNever {
		cmd := c.createNerdctlCmd(ctx, append([]string{"start"}, existingCid)...)
		if err := c.executeUpCmd(ctx, cmd, container.Name, runFlagD, service.Unparsed.StdinOpen); err != nil {
			return "", fmt.Errorf("error while starting existing container %s: %w", container.Name, err)
		}
		return existingCid, nil
	}

	// delete container if it already exists
	if existingCid != "" {
		// Default behavior for RecreateDiverged: compare stored hash with current service hash
		if recreate == RecreateDiverged {
			currentHash, err := ServiceHash(*service.Unparsed)
			if err != nil {
				return "", fmt.Errorf("failed computing service hash for %s: %w", container.Name, err)
			}
			con, err := c.client.LoadContainer(ctx, existingCid)
			if err != nil {
				return "", fmt.Errorf("failed to load container %s: %w", existingCid, err)
			}
			lbls, err := con.Labels(ctx)
			if err != nil {
				return "", fmt.Errorf("failed to read labels for %s: %w", existingCid, err)
			}
			if lbls[labels.ComposeConfigHash] == currentHash {
				cmd := c.createNerdctlCmd(ctx, append([]string{"start"}, existingCid)...)
				if err := c.executeUpCmd(ctx, cmd, container.Name, runFlagD, service.Unparsed.StdinOpen); err != nil {
					return "", fmt.Errorf("error while starting existing container %s: %w", container.Name, err)
				}
				return existingCid, nil
			}
		}
		log.G(ctx).Debugf("Container %q already exists, deleting", container.Name)
		delCmd := c.createNerdctlCmd(ctx, "rm", "-f", container.Name)
		if err = delCmd.Run(); err != nil {
			return "", fmt.Errorf("could not delete container %q: %w", container.Name, err)
		}
		log.G(ctx).Infof("Re-creating container %s", container.Name)
	} else {
		log.G(ctx).Infof("Creating container %s", container.Name)
	}

	for _, f := range container.Mkdir {
		log.G(ctx).Debugf("Creating a directory %q", f)
		if err = os.MkdirAll(f, 0o755); err != nil {
			return "", fmt.Errorf("failed to create a directory %q: %w", f, err)
		}
	}

	tempDir, err := os.MkdirTemp(os.TempDir(), "compose-")
	if err != nil {
		return "", fmt.Errorf("error while creating/re-creating container %s: %w", container.Name, err)
	}
	defer os.RemoveAll(tempDir)
	cidFilename := filepath.Join(tempDir, "cid")

	if c.EnvFile != "" {
		container.RunArgs = append([]string{"--env-file=" + c.EnvFile}, container.RunArgs...)
	}

	//add metadata labels to container https://github.com/compose-spec/compose-spec/blob/master/spec.md#labels
	currentHash, err := ServiceHash(*service.Unparsed)
	if err != nil {
		return "", fmt.Errorf("failed computing service hash for %s: %w", container.Name, err)
	}
	container.RunArgs = append([]string{
		"--cidfile=" + cidFilename,
		fmt.Sprintf("-l=%s=%s", labels.ComposeProject, c.project.Name),
		fmt.Sprintf("-l=%s=%s", labels.ComposeService, service.Unparsed.Name),
		fmt.Sprintf("-l=%s=%s", labels.ComposeConfigHash, currentHash),
	}, container.RunArgs...)

	cmd := c.createNerdctlCmd(ctx, append([]string{"run"}, container.RunArgs...)...)
	if c.DebugPrintFull {
		log.G(ctx).Debugf("Running %v", cmd.Args)
	}

	if err := c.executeUpCmd(ctx, cmd, container.Name, runFlagD, service.Unparsed.StdinOpen); err != nil {
		return "", fmt.Errorf("error while creating container %s: %w", container.Name, err)
	}

	cid, err := filesystem.ReadFile(cidFilename)
	if err != nil {
		return "", fmt.Errorf("error while creating container %s: %w", container.Name, err)
	}
	return strings.TrimSpace(string(cid)), nil
}

func (c *Composer) executeUpCmd(ctx context.Context, cmd *exec.Cmd, containerName string, runFlagD, stdinOpen bool) error {
	log.G(ctx).Infof("Running %v", cmd.Args)
	if c.DebugPrintFull {
		log.G(ctx).Debugf("Running %v", cmd.Args)
	}

	if stdinOpen {
		cmd.Stdin = os.Stdin
	}
	if !runFlagD {
		cmd.Stdout = os.Stdout
	}
	// Always propagate stderr to print detailed error messages (https://github.com/containerd/nerdctl/issues/1942)
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("error while creating container %s: %w", containerName, err)
	}

	return nil
}
