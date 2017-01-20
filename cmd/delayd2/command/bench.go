package command

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/cybozu-go/cmd"
	"github.com/kelseyhightower/envconfig"
	"github.com/nabeken/aws-go-sqs/queue"
	"github.com/nabeken/aws-go-sqs/queue/option"
	"github.com/nabeken/delayd2"
)

type BenchRecvConfig struct {
	QueueName string `envconfig:"queue_name"`

	NumberOfMessages int
}

type BenchCommand struct {
	Meta
}

func (c *BenchCommand) Recv(args []string) int {
	var config BenchRecvConfig

	if err := envconfig.Process("delayd2", &config); err != nil {
		c.Ui.Error(fmt.Sprintf("Unable to get configuration from envvars: %s", err))
		return 1
	}

	cmdFlags := flag.NewFlagSet("batch recv", flag.ContinueOnError)
	if err := cmdFlags.Parse(args); err != nil {
		c.Ui.Error(c.Help())
		return 1
	}

	sqsSvc := sqs.New(session.New())
	q, err := queue.New(sqsSvc, config.QueueName)
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Unable to initialize SQS connection: %s", err))
		return 1
	}

	log.Print("bench: --- Configuration ---")
	log.Printf("bench: DELAYD2_QUEUE_NAME=%s", config.QueueName)

	log.Print("bench: Start to drain and don't stop until you press C-c.")
	cmd.Go(func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				log.Print("bench: Stopping now...")
				return ctx.Err()
			default:
			}

			messages, err := q.ReceiveMessage(
				option.MaxNumberOfMessages(10),
				option.UseAllAttribute(),
			)

			// continue even if we get an error
			if err != nil {
				log.Printf("bench: Unable to receive messages but continuing...: %s", err)
				continue
			}

			if len(messages) == 0 {
				continue
			}

			recipientHandles := make([]*string, 0, 10)
			for _, m := range messages {
				fmt.Println(aws.StringValue(m.MessageId))
				recipientHandles = append(recipientHandles, m.ReceiptHandle)
			}

			if err := q.DeleteMessageBatch(recipientHandles...); err != nil {
				log.Printf("bench: Unable to delete message but continuing...: %s", err)
			}
		}
	})

	err = cmd.Wait()
	if err != nil && !cmd.IsSignaled(err) {
		c.Ui.Error(fmt.Sprintf("Unable to receive messages: %s", err))
		return 1
	}

	log.Print("bench: Done.")
	return 0
}

type BenchSendConfig struct {
	QueueName       string `envconfig:"queue_name"`
	TargetQueueName string `envconfig:"target_queue_name"`

	NumberOfMessages int
	Concurrency      int
	Duration         int
}

func (c *BenchCommand) Send(args []string) int {
	var config BenchSendConfig

	if err := envconfig.Process("delayd2", &config); err != nil {
		c.Ui.Error(fmt.Sprintf("Unable to get configuration from envvars: %s", err))
		return 1
	}

	cmdFlags := flag.NewFlagSet("batch send", flag.ContinueOnError)
	cmdFlags.IntVar(&config.NumberOfMessages, "n", 1000, "number of messages")
	cmdFlags.IntVar(&config.Concurrency, "c", 5, "concurrency")
	cmdFlags.IntVar(&config.Duration, "d", 10, "duration in second")

	if err := cmdFlags.Parse(args); err != nil {
		c.Ui.Error(c.Help())
		return 1
	}

	sqsSvc := sqs.New(session.New())
	q, err := queue.New(sqsSvc, config.QueueName)
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Unable to initialize SQS connection: %s", err))
		return 1
	}

	log.Print("bench: --- Configuration ---")
	log.Printf("bench: Number of messages to send: %d", config.NumberOfMessages)
	log.Printf("bench: Concurrency of sending: %d", config.Concurrency)
	log.Printf("bench: Duration of relaying: %d", config.Duration)
	log.Printf("bench: DELAYD2_QUEUE_NAME=%s", config.QueueName)
	log.Printf("bench: DELAYD2_TARGET_QUEUE_NAME=%s", config.TargetQueueName)

	s := delayd2.NewSender(q)

	log.Print("bench: Starting to send")
	payloads := make([]string, config.NumberOfMessages)
	for i := 0; i < config.NumberOfMessages; i++ {
		payloads[i] = fmt.Sprintf("%d %d", i, time.Now().Unix())
	}

	tasks := DistributeN(config.Concurrency, payloads)

	begin := time.Now()

	for i, task := range tasks {
		n := i + 1
		t := task
		cmd.Go(func(ctx context.Context) error {
			log.Printf("launching worker goroutine#%d", n)
			return s.SendMessageBatch(config.Duration, config.TargetQueueName, t)
		})
	}

	cmd.Stop()

	err = cmd.Wait()
	end := time.Now()

	if err != nil && !cmd.IsSignaled(err) {
		c.Ui.Error(fmt.Sprintf("Unable to send messages: %s", err))
		return 1
	}

	log.Printf("bench: %d messages sent in %s", len(payloads), end.Sub(begin))
	return 0
}

func (c *BenchCommand) Run(args []string) int {
	if len(args) == 0 {
		// insert default action
		args = append(args, "send")
	}

	newArgs := make([]string, len(args)-1)
	if len(args) > 1 {
		copy(newArgs, args[1:])
	}

	switch args[0] {
	case "send":
		return c.Send(newArgs)
	case "recv":
		return c.Recv(newArgs)
	}

	c.Ui.Error(fmt.Sprintf("%s is not supported", args[0]))
	return 1
}

func (c *BenchCommand) Synopsis() string {
	return ""
}

func (c *BenchCommand) Help() string {
	helpText := `

`
	return strings.TrimSpace(helpText)
}

func DistributeN(nWorkers int, payloads []string) [][]string {
	if plen := len(payloads); nWorkers > plen {
		// capped to length of payloads
		nWorkers = plen
	}

	perWorker := len(payloads) / nWorkers

	ret := make([][]string, nWorkers)
	for i := 0; i < nWorkers; i++ {
		ret[i] = make([]string, perWorker)
		copy(ret[i], payloads[:perWorker])
		payloads = payloads[perWorker:]
	}

	if mod := len(payloads) % nWorkers; mod != 0 {
		for {
			for i := 0; i < nWorkers; i++ {
				if len(payloads) == 0 {
					return ret
				}
				ret[i] = append(ret[i], payloads[0])
				payloads = payloads[1:]
			}
		}
	}

	return ret
}
