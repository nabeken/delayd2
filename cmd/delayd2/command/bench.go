package command

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/kelseyhightower/envconfig"
	"github.com/nabeken/aws-go-sqs/queue"
	"github.com/nabeken/aws-go-sqs/queue/option"
	"github.com/nabeken/delayd2"
)

type BenchSendConfig struct {
	QueueName       string `envconfig:"queue_name"`
	TargetQueueName string `envconfig:"target_queue_name"`
	Region          string `envconfig:"region"`

	NumberOfMessages int
	Concurrency      int
}

type BenchRecvConfig struct {
	QueueName string `envconfig:"queue_name"`
	Region    string `envconfig:"region"`

	NumberOfMessages int
}

type BenchCommand struct {
	Meta

	ShutdownCh <-chan struct{}
}

func (c *BenchCommand) Recv(args []string) int {
	var config BenchRecvConfig

	if err := envconfig.Process("delayd2", &config); err != nil {
		c.Ui.Error(fmt.Sprintf("Unable to get configuration from envvars: %s", err))
		return 1
	}

	cmdFlags := flag.NewFlagSet("batch recv", flag.ContinueOnError)
	cmdFlags.IntVar(&config.NumberOfMessages, "n", 1000, "number of messages")

	if err := cmdFlags.Parse(args); err != nil {
		c.Ui.Error(c.Help())
		return 1
	}

	sqsSvc := sqs.New(&aws.Config{Region: aws.String(config.Region)})
	q, err := queue.New(sqsSvc, config.QueueName)
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Unable to initialize SQS connection: %s", err))
		return 1
	}

	log.Print("bench: --- Configuration ---")
	log.Printf("bench: Number of messages to receive: %d", config.NumberOfMessages)
	log.Printf("bench: DELAYD2_QUEUE_NAME=%s", config.QueueName)

	errCh := make(chan error)

	log.Print("bench: starting")
	go func() {
		payloads := map[string]struct{}{}
		for {
			log.Printf("%d messages processed", len(payloads))
			if len(payloads) >= config.NumberOfMessages {
				break
			}

			messages, err := q.ReceiveMessage(
				option.MaxNumberOfMessages(10),
				option.UseAllAttribute(),
			)
			// continue even if we get an error
			if err != nil {
				log.Printf("unable to receive messages but continuing...: %s", err)
				continue
			}

			recipientHandles := make([]*string, 0, 10)
			for _, m := range messages {
				payloads[*m.Body] = struct{}{}
				recipientHandles = append(recipientHandles, m.ReceiptHandle)
			}

			if err := q.DeleteMessageBatch(recipientHandles...); err != nil {
				log.Printf("unable to delete message but continuing...: %s", err)
			}
		}

		log.Printf("%d messages received", config.NumberOfMessages)
		errCh <- nil
	}()

	select {
	case <-errCh:
		if err != nil {
			c.Ui.Error(fmt.Sprintf("Unable to receive messages: %s", err))
			return 1
		}
		log.Print("bench: done.")
	case <-c.ShutdownCh:
		log.Fatal("signal received.")
	}

	return 0
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

	if err := cmdFlags.Parse(args); err != nil {
		c.Ui.Error(c.Help())
		return 1
	}

	sqsSvc := sqs.New(&aws.Config{Region: aws.String(config.Region)})
	q, err := queue.New(sqsSvc, config.QueueName)
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Unable to initialize SQS connection: %s", err))
		return 1
	}

	log.Print("bench: --- Configuration ---")
	log.Printf("bench: Number of messages to send: %d", config.NumberOfMessages)
	log.Printf("bench: Concurrency of sending: %d", config.Concurrency)
	log.Printf("bench: DELAYD2_QUEUE_NAME=%s", config.QueueName)
	log.Printf("bench: DELAYD2_TARGET_QUEUE_NAME=%s", config.TargetQueueName)

	s := delayd2.NewSender(q)

	log.Print("bench: starting")
	payloads := make([]string, config.NumberOfMessages)
	for i := 0; i < config.NumberOfMessages; i++ {
		payloads[i] = fmt.Sprintf("%d %d", i, time.Now().Unix())
	}

	tasks := DistributeN(config.Concurrency, payloads)

	var wg sync.WaitGroup
	wg.Add(len(tasks))

	begin := time.Now()
	for _, task := range tasks {
		go func(t []string) {
			defer wg.Done()

			log.Print("launching worker goroutine")
			if err := s.SendMessageBatch(0, config.TargetQueueName, t); err != nil {
				log.Print(err)
				return
			}
		}(task)
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		end := time.Now()
		log.Printf("%d messages sent in %s", len(payloads), end.Sub(begin))
		close(doneCh)
	}()

	select {
	case <-doneCh:
		log.Print("bench: done.")
	case <-c.ShutdownCh:
		c.Ui.Error("signal received")
		return 1
	}

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
	if nWorkers > len(payloads) {
		// capped to length of payloads
		nWorkers = len(payloads)
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
