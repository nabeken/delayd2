package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	ccmd "github.com/cybozu-go/cmd"
	"github.com/google/gops/agent"
	"github.com/lestrrat/go-config/env"
	sqsqueue "github.com/nabeken/aws-go-sqs/queue"
	"github.com/nabeken/delayd2/database"
	"github.com/nabeken/delayd2/queue"
	"github.com/nabeken/delayd2/worker"
	"github.com/pkg/errors"
)

type ServerCommand struct {
}

type ServerConfig struct {
	WorkerID string `envconfig:"worker_id"`

	DSN       string `envconfig:"dsn"`
	QueueName string `envconfig:"queue_name"`

	ShutdownDuration int `envconfig:"shutdown_duration"`
}

func (cmd *ServerCommand) Execute(args []string) error {
	var config ServerConfig
	if err := env.NewDecoder(env.System).Prefix("DELAYD2").Decode(&config); err != nil {
		return errors.Wrap(err, "failed to load the configuration")
	}

	if config.ShutdownDuration == 0 {
		config.ShutdownDuration = 60
	}

	if config.WorkerID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return errors.Wrap(err, "unable to get hostname")
		}
		log.Printf("delayd2: No worker-id specified so use hostname (%s) instead", hostname)
		config.WorkerID = hostname
	}

	db, err := sql.Open("postgres-delayd2", config.DSN)
	if err != nil {
		return errors.Wrap(err, "unable to open the database connection")
	}
	if err := db.Ping(); err != nil {
		return errors.Wrap(err, "unable to ping the database")
	}
	defer db.Close()

	drv := database.NewDriver(config.WorkerID, db)

	awsConf := aws.NewConfig().WithHTTPClient(&http.Client{
		Timeout: 1 * time.Minute,
	})
	awsSess := session.Must(session.NewSession(awsConf))
	sqsSvc := sqs.New(awsSess)

	q, err := sqsqueue.New(sqsSvc, config.QueueName)
	if err != nil {
		return errors.Wrap(err, "unable to initialize SQS connection")
	}

	consumer := queue.NewConsumer(config.WorkerID, drv, q)
	relay := queue.NewRelay(sqsSvc)

	e := ccmd.NewEnvironment(context.Background())
	w := worker.New(e, drv, consumer, relay)

	go func() {
		agent.Listen(&agent.Options{NoShutdownCleanup: true})
		// we don't close the agent to diagnostic within shutdown procedures too
	}()

	e.Go(func(_ context.Context) error {
		log.Println("delayd2: starting the server...")
		return w.Run()
	})

	// wait for signals
	ccmd.Go(func(ctx context.Context) error {
		<-ctx.Done()
		e.Cancel(nil)

		log.Println("delayd2: shutting down the server...")
		duration := time.Duration(config.ShutdownDuration) * time.Second

		// we should wait for config.ShutdownDuration seconds.
		ctx, cancel := context.WithTimeout(context.Background(), duration)
		err = w.Shutdown(ctx)
		cancel()
		return err
	})

	// wait for all delayd2 goroutine to be done
	err = e.Wait()
	if err != nil && !ccmd.IsSignaled(err) {
		return errors.Wrap(err, "unable to launch or shutdown properly")
	}

	log.Printf("delayd2: shutdown completed")

	return nil
}

func init() {
	parser.AddCommand(
		"server",
		"Run delayd2 server",
		"The server command runs delayd2 server in foreground.",
		&ServerCommand{},
	)
}
