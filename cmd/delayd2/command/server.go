package command

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/cybozu-go/cmd"
	"github.com/google/gops/agent"
	"github.com/kelseyhightower/envconfig"
	"github.com/nabeken/aws-go-sqs/queue"
	"github.com/nabeken/delayd2"
)

type ServerConfig struct {
	WorkerID  string `envconfig:"worker_id"`
	DSN       string `envconfig:"dsn"`
	QueueName string `envconfig:"queue_name"`

	ShutdownDuration  int `envconfig:"shutdown_duration"`
	NumConsumerFactor int `envconfig:"num_consumer_factor"`
	NumRelayFactor    int `envconfig:"num_relay_factor"`

	LeaveMessagesOrphanedAtShutdown bool
}

type ServerCommand struct {
	Meta
}

func (c *ServerCommand) Run(args []string) int {
	var config ServerConfig

	if err := envconfig.Process("delayd2", &config); err != nil {
		c.Ui.Error(fmt.Sprintf("Unable to get configuration from envvars: %s", err))
		return 1
	}

	cmdFlags := flag.NewFlagSet("server", flag.ContinueOnError)
	cmdFlags.BoolVar(&config.LeaveMessagesOrphanedAtShutdown, "leave-messages-orphaned-at-shutdown", true, "leave messages orphaned at shutdown")

	if err := cmdFlags.Parse(args); err != nil {
		c.Ui.Error(c.Help())
		return 1
	}

	// initialize configuration
	if config.NumConsumerFactor == 0 {
		config.NumConsumerFactor = 1
	}

	if config.NumRelayFactor == 0 {
		config.NumRelayFactor = 1
	}

	if config.ShutdownDuration == 0 {
		config.ShutdownDuration = 60
	}

	if config.WorkerID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			c.Ui.Error(fmt.Sprintf("Unable to get hostname: %s", err))
			return 1
		}
		log.Printf("No worker-id specified so use hostname (%s) instead", hostname)
		config.WorkerID = hostname
	}

	db, err := sql.Open("postgres-delayd2", config.DSN)
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Unable to open database connection: %s", err))
		return 1
	}
	if err := db.Ping(); err != nil {
		c.Ui.Error(fmt.Sprintf("Unable to open database connection: %s", err))
		return 1
	}
	defer db.Close()

	drv := delayd2.NewDriver(config.WorkerID, db)
	conf := aws.NewConfig().WithHTTPClient(&http.Client{
		Timeout: 1 * time.Minute,
	})
	sqsSvc := sqs.New(session.New(conf))

	q, err := queue.New(sqsSvc, config.QueueName)
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Unable to initialize SQS connection: %s", err))
		return 1
	}

	consumer := delayd2.NewConsumer(config.WorkerID, drv, q)
	relay := delayd2.NewRelay(sqsSvc)

	workerConfig := &delayd2.WorkerConfig{
		ID: config.WorkerID,

		NumConsumerFactor: config.NumConsumerFactor,
		NumRelayFactor:    config.NumRelayFactor,

		LeaveMessagesOrphanedAtShutdown: config.LeaveMessagesOrphanedAtShutdown,
	}

	e := cmd.NewEnvironment(context.Background())
	w := delayd2.NewWorker(e, workerConfig, drv, consumer, relay)

	e.Go(func(ctx context.Context) error {
		return agent.Listen(&agent.Options{
			NoShutdownCleanup: true,
		})
		// we don't close the agent to diagnostic within shutdown procedures too
	})

	e.Go(func(ctx context.Context) error {
		return w.Run()
	})

	cmd.Go(func(ctx context.Context) error {
		<-ctx.Done()
		e.Cancel(nil)

		log.Println("Shutting down the server...")
		duration := time.Duration(config.ShutdownDuration) * time.Second

		// we should wait for config.ShutdownDuration seconds.
		ctx, cancel := context.WithTimeout(context.Background(), duration)
		err = w.Shutdown(ctx)
		cancel()
		return err
	})

	err = cmd.Wait()
	if err != nil && !cmd.IsSignaled(err) {
		c.Ui.Error(fmt.Sprintf("Unable to shutdown: %s", err.Error()))
		return 1
	}

	log.Printf("Shutdown completed")
	return 0
}

func (c *ServerCommand) Synopsis() string {
	return ""
}

func (c *ServerCommand) Help() string {
	helpText := `

`
	return strings.TrimSpace(helpText)
}
