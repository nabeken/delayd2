package command

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/kelseyhightower/envconfig"
	"github.com/nabeken/aws-go-sqs/queue"
	"github.com/nabeken/delayd2"
)

type ServerConfig struct {
	WorkerID  string `envconfig:"worker_id"`
	DSN       string `envconfig:"dsn"`
	QueueName string `envconfig:"queue_name"`
	Region    string `envconfig:"region"`
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

	db, err := sql.Open("postgres", config.DSN)
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
	sqsSvc := sqs.New(&aws.Config{Region: aws.String(config.Region)})

	q, err := queue.New(sqsSvc, config.QueueName)
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Unable to initialize SQS connection: %s", err))
		return 1
	}

	consumer := delayd2.NewConsumer(config.WorkerID, drv, q)
	relay := delayd2.NewRelay(sqsSvc)

	w := delayd2.NewWorker(config.WorkerID, drv, consumer, relay)

	errCh := make(chan error)
	go func() {
		errCh <- w.Run()
	}()

	go func() {
		select {
		case <-c.ShutdownCh:
			// we should wait until s.Stop returns for 10 seconds.
			time.AfterFunc(10*time.Second, func() {
				errCh <- errors.New("delayd2: Worker#Stop() does not return for 10 seconds. existing...")
			})
			w.Stop()
		}
	}()

	select {
	case err := <-errCh:
		if err != nil {
			c.Ui.Error(err.Error())
			return 1
		}
	}
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
