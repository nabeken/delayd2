package command

import (
	"testing"

	"github.com/mitchellh/cli"
)

func TestQueueCommand_implement(t *testing.T) {
	var _ cli.Command = &QueueCommand{}
}
