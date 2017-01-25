package main

import (
	"github.com/mitchellh/cli"
	"github.com/nabeken/delayd2/cmd/delayd2/command"
)

func Commands(meta *command.Meta) map[string]cli.CommandFactory {
	return map[string]cli.CommandFactory{
		"server": func() (cli.Command, error) {
			return &command.ServerCommand{
				Meta: *meta,
			}, nil
		},
		"bench": func() (cli.Command, error) {
			return &command.BenchCommand{
				Meta: *meta,
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
