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
	updateStatus(id int, status string, t time.Time)
}

type defaultStatus struct{}

func (defaultStatus) updateStatus(id int, status string, t time.Time) {
	log.Printf("[DEFAULT] Task id: %d Task status: %s Task time: %s\n", id, status, t.Format(time.RFC3339))
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
			a.status.updateStatus(id, "STOPPED", t)
			return

		case t := <-ticker.C:
			a.status.updateStatus(id, "RUNNING", t)
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

func (a *App) RunTasks(
	ctx context.Context,
	id int,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	log.Printf("Task %d started\n", id)

	// отдельный контекст heartbeat
	hbCtx, hbCancel := context.WithCancel(ctx)

	// waitgroup heartbeat goroutine
	var hbWG sync.WaitGroup

	// канал ошибок heartbeat
	errCh := make(chan error, 1)

	// старт heartbeat
	a.startHeartbeat(
		hbCtx,
		id,
		errCh,
		&hbWG,
	)

	for _, task := range a.tasks {
		task(id, a.logger)
		// проверяем heartbeat
		a.ensureHeartbeat(
			hbCtx,
			id,
			errCh,
			&hbWG,
		)
	}

	// останавливаем heartbeat
	hbCancel()

	// ждём завершения heartbeat goroutines
	hbWG.Wait()

	msg := fmt.Sprintf("Task %d completed\n",
		id)
	a.logger.Log(msg, "INFO")
}
