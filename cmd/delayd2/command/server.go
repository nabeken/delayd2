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

	_ "net/http/pprof"

	_ "github.com/lib/pq"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/fukata/golang-stats-api-handler"
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

	HTTPServer string

	LeaveMessagesOrphanedAtShutdown bool
}

type ServerCommand struct {
	Meta

	ShutdownCh <-chan struct{}
}

func (c *ServerCommand) Run(args []string) int {
	var config ServerConfig

	if err := envconfig.Process("delayd2", &config); err != nil {
		c.Ui.Error(fmt.Sprintf("Unable to get configuration from envvars: %s", err))
		return 1
	}

	cmdFlags := flag.NewFlagSet("server", flag.ContinueOnError)
	cmdFlags.StringVar(&config.HTTPServer, "http-server", "127.0.0.1:6060", "Listen HTTP request for pprof and stats on 127.0.0.1:6060")
	cmdFlags.BoolVar(&config.LeaveMessagesOrphanedAtShutdown, "leave-messages-orphaned-at-shutdown", true, "leave messages orphaned at shutdown")

	if err := cmdFlags.Parse(args); err != nil {
		c.Ui.Error(c.Help())
		return 1
	}

	if config.HTTPServer != "" {
		http.HandleFunc("/_stats", stats_api.Handler)
		log.Println("server: launching http server on ", config.HTTPServer)
		go func() {
			log.Println(http.ListenAndServe(config.HTTPServer, nil))
		}()
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
	sqsSvc := sqs.New(session.New())

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

	w := delayd2.NewWorker(workerConfig, drv, consumer, relay)

	// run in another goroutine
	go func() { w.Run() }()

	baseCtx := context.Background()
	duration := time.Duration(config.ShutdownDuration) * time.Second

	select {
	case <-c.ShutdownCh:
		// we should wait for config.ShutdownDuration seconds.
		ctx, cancel := context.WithTimeout(baseCtx, duration)
		err = w.Shutdown(ctx)
		cancel()
	}

	if err != nil {
		c.Ui.Error(fmt.Sprintf("Unable to shutdown within %s: %s", duration, err.Error()))
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
