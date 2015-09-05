package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
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

	delayd := delayd2.New("worker-1", db)
	n, err := delayd.MarkActive(time.Now())
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
		fmt.Println(m)
		if err := delayd.RemoveMessage(m.ID); err != nil {
			log.Println(err)
		}
		fmt.Printf("queue_id %s is removed from the queue\n", m.ID)
	}
}
