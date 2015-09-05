package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/nabeken/delayd2"
)

func main() {
	db, err := sql.Open("postgres", "dbname=delayd2 sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	queueName := os.Getenv("TEST_SQS_QUEUE_NAME")
	if err != nil {
		log.Fatal(err)
	}

	consumer, err := delayd2.NewSQSConsumer(
		"worker-1",
		db,
		sqs.New(&aws.Config{Region: aws.String("ap-northeast-1")}),
		queueName,
	)
	if err != nil {
		log.Fatal(err)
	}
	if err := consumer.ConsumeMessages(); err != nil {
		log.Fatal(err)
	}

	delayd := delayd2.New("worker-1", db)

	n, err := delayd.ResetActive()
	if err != nil {
		log.Fatal(err)
	}
	if n > 0 {
		fmt.Printf("%d active messages found. resetting...\n", n)
	}

	n, err = delayd.MarkActive(time.Now())
	if err != nil {
		log.Fatal(err)
	}

	if n > 0 {
		fmt.Printf("%d messages are moved to active\n", n)
	} else {
		fmt.Println("no messages are in active")
		return
	}

	activeMessages, err := delayd.GetActiveMessages()
	if err != nil {
		log.Fatal(err)
	}
	for _, m := range activeMessages {
		fmt.Println(string(m.Payload))
		if err := delayd.RemoveMessage(m.ID); err != nil {
			log.Println(err)
		}
		fmt.Printf("queue_id %s is removed from the queue\n", m.ID)
	}
}
