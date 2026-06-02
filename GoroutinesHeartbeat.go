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
	Log(message, level string)
}

type defaultLogger struct{}

func (defaultLogger) Log(message, level string) {
	log.Printf("[DEFAULT] level: %s message: %s\n", level, message)
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
	logger.Log(
		fmt.Sprintf(
			"[DEFAULT] Task id: %d Task status: %s Task time: %s",
			id,
			status,
			t.Format(time.RFC3339),
		),
		"INFO",
	)
	if id == 1 {
		panic(1)
	}
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
) {

	ticker := time.NewTicker(5 * time.Second)
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
				a.logger.Log(msg, "ERROR")

				errCh <- err
			}
		}()

		a.taskHeartbeat(ctx, id)
	}()
}

func (a *App) ensureHeartbeat(
	ctx context.Context,
	id int,
	errCh chan error,
	wg *sync.WaitGroup,
) {
	select {

	case err := <-errCh:

		msg := fmt.Sprintf("Heartbeat crashed for task %d: %v",
			id,
			err)

		a.logger.Log(msg, "WARNING")

		msg = fmt.Sprintf("Restarting heartbeat for task %d",
			id)

		a.logger.Log(msg, "WARNING")

		a.startHeartbeat(
			ctx,
			id,
			errCh,
			wg,
		)

	default:

		msg := fmt.Sprintf("Heartbeat alive for task %d",
			id)

		a.logger.Log(msg, "INFO")
	}
}

func (a *App) RunTask(id int) {
	go func() {
		ctx := context.Background()

		a.logger.Log(fmt.Sprintf("Task %d started\n", id),
			"INFO")

		hbCtx, hbCancel := context.WithCancel(ctx)

		var hbWG sync.WaitGroup
		errCh := make(chan error, 1)

		a.startHeartbeat(
			hbCtx,
			id,
			errCh,
			&hbWG,
		)

		for _, task := range a.tasks {
			task(id, a.logger)

			a.ensureHeartbeat(
				hbCtx,
				id,
				errCh,
				&hbWG,
			)
		}

		hbCancel()
		hbWG.Wait()

		a.logger.Log(
			fmt.Sprintf("Task %d completed\n", id),
			"INFO",
		)
	}()
}
