package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/mitchellh/cli"
	"github.com/nabeken/delayd2/cmd/delayd2/command"
)

func Commands(meta *command.Meta) map[string]cli.CommandFactory {
	return map[string]cli.CommandFactory{
		"server": func() (cli.Command, error) {
			return &command.ServerCommand{
				Meta:       *meta,
				ShutdownCh: makeShutdownCh(),
			}, nil
		},
		"bench": func() (cli.Command, error) {
			return &command.BenchCommand{
				Meta:       *meta,
				ShutdownCh: makeShutdownCh(),
			}, nil
		},
		"session": func() (cli.Command, error) {
			return &command.SessionCommand{
				Meta: *meta,
			}, nil
		},
		"queue": func() (cli.Command, error) {
			return &command.QueueCommand{
				Meta: *meta,
			}, nil
		},

		"version": func() (cli.Command, error) {
			return &command.VersionCommand{
				Meta:     *meta,
				Version:  Version,
				Revision: GitCommit,
				Name:     Name,
			}, nil
		},
	}
}

func makeShutdownCh() <-chan struct{} {
	ch := make(chan struct{})

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		for {
			<-sigCh
			ch <- struct{}{}
		}
	}()

	return ch
}
