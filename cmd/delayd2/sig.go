package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nabeken/delayd2"
)

// installSigHandler installs a signal handler to shutdown gracefully for ^C and kill
func installSigHandler(w *delayd2.Worker) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-ch:
			// we should wait until s.Stop returns for 10 seconds.
			time.AfterFunc(10*time.Second, func() {
				log.Print("delayd2: Worker#Stop() does not return for 10 seconds. existing...")
				os.Exit(0)
			})
			w.Stop()
		}
	}()
}
