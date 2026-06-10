package goroutinesheartbeat

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"time"
)

type Logger interface {
	Info(message string)
	Error(message string)
	Warning(message string)
}

type defaultLogger struct{}

func (defaultLogger) Info(message string) {
	log.Printf("[DEFAULT] level: %s message: %s\n", "INFO", message)
}

func (defaultLogger) Error(message string) {
	log.Printf("[DEFAULT] level: %s message: %s\n", "ERROR", message)
}

func (defaultLogger) Warning(message string) {
	log.Printf("[DEFAULT] level: %s message: %s\n", "WARNING", message)
}

type Status interface {
	updateStatus(
		id int,
		status string,
		t time.Time,
		logger Logger)
}

type StatusFunc func(
	id int,
	status string,
	t time.Time,
	logger Logger,
)

func (f StatusFunc) updateStatus(
	id int,
	status string,
	t time.Time,
	logger Logger,
) {
	f(id, status, t, logger)
}

type defaultStatus struct{}

func (defaultStatus) updateStatus(
	id int,
	status string,
	t time.Time,
	logger Logger,
) {
	logger.Info(
		fmt.Sprintf(
			"[DEFAULT] Task id: %d Task status: %s Task time: %s",
			id,
			status,
			t.Format(time.RFC3339)))
}

type TaskFunc func(id int, logger Logger)

type App struct {
	logger Logger
	status Status
	tasks  []TaskFunc
}

func New(
	logger Logger,
	status Status,
	tasks []TaskFunc,
) *App {
	app := &App{}

	if logger == nil {
		app.logger = defaultLogger{}
	} else {
		app.logger = logger
	}

	if status == nil {
		app.status = defaultStatus{}
	} else {
		app.status = status
	}

	app.tasks = tasks
	return app
}

func (a *App) taskHeartbeat(
	ctx context.Context,
	id int,
	heartBeatSeconds int,
) {

	ticker := time.NewTicker(time.Duration(heartBeatSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t := <-ticker.C
			a.status.updateStatus(id, "STOPPED", t, a.logger)
			return

		case t := <-ticker.C:
			a.status.updateStatus(id, "RUNNING", t, a.logger)
		}
	}
}

func (a *App) startHeartbeat(
	ctx context.Context,
	id int,
	errCh chan<- error,
	wg *sync.WaitGroup,
	heartBeatSeconds int,
) {
	wg.Add(1)

	go func() {
		defer wg.Done()

		defer func() {
			if r := recover(); r != nil {

				err := fmt.Errorf("%v", r)

				msg := fmt.Sprintf(
					"heartbeat panic recovered: %v\n%s",
					err,
					debug.Stack())
				a.logger.Error(msg)

				errCh <- err
			}
		}()

		a.taskHeartbeat(ctx, id, heartBeatSeconds)
	}()
}

func (a *App) ensureHeartbeat(
	ctx context.Context,
	id int,
	errCh chan error,
	wg *sync.WaitGroup,
	heartBeatSeconds int,
) {
	select {

	case err := <-errCh:

		msg := fmt.Sprintf("Heartbeat crashed for task %d: %v",
			id,
			err)

		a.logger.Warning(msg)

		msg = fmt.Sprintf("Restarting heartbeat for task %d",
			id)

		a.logger.Warning(msg)

		a.startHeartbeat(
			ctx,
			id,
			errCh,
			wg,
			heartBeatSeconds,
		)

	default:

		msg := fmt.Sprintf("Heartbeat alive for task %d",
			id)

		a.logger.Info(msg)
	}
}

func (a *App) RunTask(id int, heartBeatSeconds int) {
	go func() {
		ctx := context.Background()

		a.logger.Info(fmt.Sprintf("Task %d started\n", id))

		hbCtx, hbCancel := context.WithCancel(ctx)

		var hbWG sync.WaitGroup
		errCh := make(chan error, 1)

		a.startHeartbeat(
			hbCtx,
			id,
			errCh,
			&hbWG,
			heartBeatSeconds,
		)

		for _, task := range a.tasks {
			task(id, a.logger)

			a.ensureHeartbeat(
				hbCtx,
				id,
				errCh,
				&hbWG,
				heartBeatSeconds,
			)
		}

		hbCancel()
		hbWG.Wait()

		a.logger.Info(
			fmt.Sprintf("Task %d completed\n", id))
	}()
}
