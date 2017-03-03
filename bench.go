package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	ccmd "github.com/cybozu-go/cmd"
	"github.com/lestrrat/go-config/env"
	sqsqueue "github.com/nabeken/aws-go-sqs/queue"
	"github.com/nabeken/delayd2/queue"
	"github.com/pkg/errors"
)

type BenchCommand struct {
}

type BenchSendCommand struct {
	NumberOfMessages int `short:"n" default:"1000" description:"the number of message to send"`
	Concurrency      int `short:"c" default:"5" description:"the number of concurrency"`
	Duration         int `short:"d" default:"10" description:"the duration in second"`
}

type BenchSendConfig struct {
	QueueName       string `envconfig:"queue_name"`
	TargetQueueName string `envconfig:"target_queue_name"`
}

func (cmd *BenchSendCommand) Execute(args []string) error {
	var config BenchSendConfig
	if err := env.NewDecoder(env.System).Prefix("DELAYD2").Decode(&config); err != nil {
		return errors.Wrap(err, "failed to load the configuration")
	}

	awsConf := aws.NewConfig().WithHTTPClient(&http.Client{
		Timeout: 1 * time.Minute,
	})
	awsSess := session.Must(session.NewSession(awsConf))
	sqsSvc := sqs.New(awsSess)

	q, err := sqsqueue.New(sqsSvc, config.QueueName)
	if err != nil {
		return errors.Wrap(err, "unable to initialize SQS connection")
	}

	log.Printf("bench: --- Configuration ---")
	log.Printf("bench: Number of messages to send: %d", cmd.NumberOfMessages)
	log.Printf("bench: Concurrency of sending: %d", cmd.Concurrency)
	log.Printf("bench: Duration of relaying: %d", cmd.Duration)
	log.Printf("bench: DELAYD2_QUEUE_NAME=%s", config.QueueName)
	log.Printf("bench: DELAYD2_TARGET_QUEUE_NAME=%s", config.TargetQueueName)

	s := queue.NewSender(q)

	log.Printf("bench: starting to send")
	payloads := make([]string, cmd.NumberOfMessages)
	for i := 0; i < cmd.NumberOfMessages; i++ {
		payloads[i] = fmt.Sprintf("%d %d", i, time.Now().Unix())
	}

	tasks := distributeN(cmd.Concurrency, payloads)
	begin := time.Now()

	for i, task := range tasks {
		n := i + 1
		t := task
		ccmd.Go(func(_ context.Context) error {
			log.Printf("bench: launching worker goroutine#%d", n)
			return s.SendMessageBatch(cmd.Duration, config.TargetQueueName, t)
		})
	}

	ccmd.Stop()

	err = ccmd.Wait()
	end := time.Now()

	if err != nil && !ccmd.IsSignaled(err) {
		return errors.Wrap(err, "unable to send messages")
	}

	log.Printf("bench: %d messages sent in %s", len(payloads), end.Sub(begin))

	return nil
}

func distributeN(nWorkers int, payloads []string) [][]string {
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

func init() {
	cmd, _ := parser.AddCommand(
		"bench",
		"Run benchmarking",
		"The bench command is for benchmakering for delayd2.",
		&BenchCommand{},
	)
	cmd.AddCommand(
		"send",
		"Run benchmarking",
		"The bench send command sends messages to SQS.",
		&BenchSendCommand{},
	)
}
