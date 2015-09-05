package main

import (
	"database/sql"
	"log"
	"os"

	_ "github.com/lib/pq"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/nabeken/aws-go-sqs/queue"
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

	workerID := "worker-1"

	drv := delayd2.NewDriver(workerID, db)
	sqsSvc := sqs.New(&aws.Config{Region: aws.String("ap-northeast-1")})

	q, err := queue.New(sqsSvc, queueName)
	if err != nil {
		log.Fatal(err)
	}

	consumer := delayd2.NewConsumer(workerID, drv, q)

	w := delayd2.NewWorker(workerID, drv, consumer)
	installSigHandler(w)

	if err := w.Run(); err != nil {
		log.Print(err)
	}
	log.Println("delayd2: shutdown completed")
}
