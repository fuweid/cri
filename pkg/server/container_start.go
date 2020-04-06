/*
Copyright 2017 The Kubernetes Authors.

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

package server

import (
	"io"
	"os"
	"time"

	"github.com/containerd/containerd"
	containerdio "github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/errdefs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	runtime "k8s.io/kubernetes/pkg/kubelet/apis/cri/runtime/v1alpha2"

	ctrdutil "github.com/containerd/cri/pkg/containerd/util"
	cioutil "github.com/containerd/cri/pkg/ioutil"
	cio "github.com/containerd/cri/pkg/server/io"
	containerstore "github.com/containerd/cri/pkg/store/container"
	sandboxstore "github.com/containerd/cri/pkg/store/sandbox"
)

// StartContainer starts the container.
func (c *criService) StartContainer(ctx context.Context, r *runtime.StartContainerRequest) (retRes *runtime.StartContainerResponse, retErr error) {
	cntr, err := c.containerStore.Get(r.GetContainerId())
	if err != nil {
		return nil, errors.Wrapf(err, "an error occurred when try to find container %q", r.GetContainerId())
	}

	id := cntr.ID
	meta := cntr.Metadata
	container := cntr.Container
	config := meta.Config

	// Set starting state to prevent other start/remove operations against this container
	// while it's being started.
	if err := setContainerStarting(cntr); err != nil {
		return nil, errors.Wrapf(err, "failed to set starting state for container %q", id)
	}
	defer func() {
		if retErr != nil {
			// Set container to exited if fail to start.
			if err := cntr.Status.UpdateSync(func(status containerstore.Status) (containerstore.Status, error) {
				status.Pid = 0
				status.FinishedAt = time.Now().UnixNano()
				status.ExitCode = errorStartExitCode
				status.Reason = errorStartReason
				status.Message = retErr.Error()
				return status, nil
			}); err != nil {
				logrus.WithError(err).Errorf("failed to set start failure state for container %q", id)
			}
		}
		if err := resetContainerStarting(cntr); err != nil {
			logrus.WithError(err).Errorf("failed to reset starting state for container %q", id)
		}
	}()

	// Get sandbox config from sandbox store.
	sandbox, err := c.sandboxStore.Get(meta.SandboxID)
	if err != nil {
		return nil, errors.Wrapf(err, "sandbox %q not found", meta.SandboxID)
	}
	sandboxID := meta.SandboxID
	if sandbox.Status.Get().State != sandboxstore.StateReady {
		return nil, errors.Errorf("sandbox container %q is not running", sandboxID)
	}

	// NOTE: There are two cases:
	//
	// 1. For create state, ContainerIO has been initialized during
	// restart or CreateContainer. For precreated fifo, we have to close it
	// to prevent fd or goroutine leaky.
	//
	// 2. For exited state, the event monitor will close ContainerIO after
	// containerd.Task. Just in case, we can do second round close.
	//
	// Unknown state doesn't have ContainerIO but we can't restart unknown
	// stated container. No need to worry about it.
	if cntr.IO != nil {
		cntr.IO.Close()
	}

	containerIO, err := cio.NewContainerIO(id,
		cio.WithNewFIFOs(c.getVolatileContainerRootDir(id), meta.Config.GetTty(), meta.Config.GetStdin()),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create container io")
	}

	ioCreation := func(id string) (_ containerdio.IO, err error) {
		stdoutWC, stderrWC, err := c.createContainerLoggers(meta.LogPath, config.GetTty())
		if err != nil {
			return nil, errors.Wrap(err, "failed to create container loggers")
		}
		containerIO.AddOutput("log", stdoutWC, stderrWC)
		containerIO.Pipe()
		return containerIO, nil
	}
	defer func() {
		if retErr != nil {
			if err := containerIO.Close(); err != nil {
				logrus.WithError(err).Errorf("failed to close container io %q", id)
			}
		}
	}()

	ctrInfo, err := container.Info(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get container info")
	}

	var taskOpts []containerd.NewTaskOpts
	// TODO(random-liu): Remove this after shim v1 is deprecated.
	if c.config.NoPivot && ctrInfo.Runtime.Name == linuxRuntime {
		taskOpts = append(taskOpts, containerd.WithNoPivotRoot)
	}
	task, err := container.NewTask(ctx, ioCreation, taskOpts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create containerd task")
	}
	defer func() {
		if retErr != nil {
			deferCtx, deferCancel := ctrdutil.DeferContext()
			defer deferCancel()
			// It's possible that task is deleted by event monitor.
			if _, err := task.Delete(deferCtx, containerd.WithProcessKill); err != nil && !errdefs.IsNotFound(err) {
				logrus.WithError(err).Errorf("Failed to delete containerd task %q", id)
			}
		}
	}()

	// wait is a long running background request, no timeout needed.
	exitCh, err := task.Wait(ctrdutil.NamespacedContext())
	if err != nil {
		return nil, errors.Wrap(err, "failed to wait for containerd task")
	}

	// Start containerd task.
	if err := task.Start(ctx); err != nil {
		return nil, errors.Wrapf(err, "failed to start containerd task %q", id)
	}

	status := containerstore.Status{
		CreatedAt: cntr.Status.Get().CreatedAt,
		Pid:       task.Pid(),
		StartedAt: time.Now().UnixNano(),
	}
	newCntr, err := containerstore.NewContainer(meta,
		containerstore.WithStatus(status, c.getContainerRootDir(id)),
		containerstore.WithContainer(container),
		containerstore.WithContainerIO(containerIO),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to recreate internal container object for %q", id)
	}

	// NOTE: In containerstore, we can't update object directly because
	//
	// 1. StopCh cann't be closed if the container is EXITED.
	// 2. ContainerIO has been closed and cann't be reused for restart.
	// 3. To avoid data race, object should be replaced entirely.
	//
	// containerd.Container represents the lifecycle of container, including
	// snapshotter data. No need to recreate it and just reuse it.
	c.containerStore.Update(newCntr)
	// start the monitor after updating container state, this ensures that
	// event monitor receives the TaskExit event and update container state
	// after this.
	c.eventMonitor.startExitMonitor(context.Background(), id, task.Pid(), exitCh)

	return &runtime.StartContainerResponse{}, nil
}

// setContainerStarting sets the container into starting state. In starting state, the
// container will not be removed or started again.
func setContainerStarting(container containerstore.Container) error {
	return container.Status.Update(func(status containerstore.Status) (containerstore.Status, error) {
		// Return error if container is not in created state.
		if !(status.State() == runtime.ContainerState_CONTAINER_CREATED || status.State() == runtime.ContainerState_CONTAINER_EXITED) {
			return status, errors.Errorf("container is in %s state", criContainerStateToString(status.State()))
		}
		// Do not start the container when there is a removal in progress.
		if status.Removing {
			return status, errors.New("container is in removing state, can't be started")
		}
		if status.Starting {
			return status, errors.New("container is already in starting state")
		}
		status.Starting = true
		return status, nil
	})
}

// resetContainerStarting resets the container starting state on start failure. So
// that we could remove the container later.
func resetContainerStarting(container containerstore.Container) error {
	return container.Status.Update(func(status containerstore.Status) (containerstore.Status, error) {
		status.Starting = false
		return status, nil
	})
}

// createContainerLoggers creates container loggers and return write closer for stdout and stderr.
func (c *criService) createContainerLoggers(logPath string, tty bool) (stdout io.WriteCloser, stderr io.WriteCloser, err error) {
	if logPath != "" {
		// Only generate container log when log path is specified.
		f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to create and open log file")
		}
		defer func() {
			if err != nil {
				f.Close()
			}
		}()
		var stdoutCh, stderrCh <-chan struct{}
		wc := cioutil.NewSerialWriteCloser(f)
		stdout, stdoutCh = cio.NewCRILogger(logPath, wc, cio.Stdout, c.config.MaxContainerLogLineSize)
		// Only redirect stderr when there is no tty.
		if !tty {
			stderr, stderrCh = cio.NewCRILogger(logPath, wc, cio.Stderr, c.config.MaxContainerLogLineSize)
		}
		go func() {
			if stdoutCh != nil {
				<-stdoutCh
			}
			if stderrCh != nil {
				<-stderrCh
			}
			logrus.Debugf("Finish redirecting log file %q, closing it", logPath)
			f.Close()
		}()
	} else {
		stdout = cio.NewDiscardLogger()
		stderr = cio.NewDiscardLogger()
	}
	return
}
