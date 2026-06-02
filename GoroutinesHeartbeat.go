package goroutinesheartbeat

import (
	//"context"
	"log"
	//"runtime/debug"
	//"sync"
	//"time"
)

type Logger interface {
	Log(message, level string)
}

type defaultLogger struct{}

func (defaultLogger) Log(message, level string) {
	log.Printf("[DEFAULT %s] %s\n", level, message)
}

type App struct {
	logger Logger
}

func (a *App) Test() {
	a.logger.Log("test", "INFO")
}

/*
func taskHeartbeat(ctx context.Context, id int) {

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t := <-ticker.C
			updateStatus(id, "STOPPED", t)
			return

		case t := <-ticker.C:
			updateStatus(id, "RUNNING", t)
		}
	}
}

func startHeartbeat(
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

				log.Printf(
					"heartbeat panic recovered: %v\n%s",
					err,
					debug.Stack(),
				)

				errCh <- err
			}
		}()

		taskHeartbeat(ctx, id)
	}()
}

func ensureHeartbeat(ctx context.Context, id int, errCh chan error, wg *sync.WaitGroup,
) {
	select {

	case err := <-errCh:

		log.Printf(
			"Heartbeat crashed for task %d: %v",
			id,
			err,
		)

		log.Printf(
			"Restarting heartbeat for task %d",
			id,
		)

		startHeartbeat(
			ctx,
			id,
			errCh,
			wg,
		)

	default:

		log.Printf(
			"Heartbeat alive for task %d",
			id,
		)
	}
}

// Заменить на сохранение в БД
func updateStatus(id int, status string, t time.Time) {
	log.Printf("Task %d %s at %s\n", id, status, t.Format(time.RFC3339))
	if id == 1 {
		panic(1)
	}
}

func longTask(
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
	startHeartbeat(
		hbCtx,
		id,
		errCh,
		&hbWG,
	)

	// работа №1
	serverTask(id)

	// проверяем heartbeat
	ensureHeartbeat(
		hbCtx,
		id,
		errCh,
		&hbWG,
	)

	// работа №2
	serverTask(id)

	// ещё раз проверяем heartbeat
	ensureHeartbeat(
		hbCtx,
		id,
		errCh,
		&hbWG,
	)

	// останавливаем heartbeat
	hbCancel()

	// ждём завершения heartbeat goroutines
	hbWG.Wait()

	log.Printf(
		"Task %d completed\n",
		id,
	)
}

// Заменить на задание на сервере
func serverTask(id int) {
	time.Sleep(15 * time.Second)
	log.Printf("✅ Task %d finished\n", id)
}

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	const tasks = 5

	for i := 1; i <= tasks; i++ {
		wg.Add(1)
		go longTask(ctx, i, &wg)
	}

	wg.Wait()

	log.Println("All tasks completed")
}*/
