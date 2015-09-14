package command

import (
	"testing"

	"github.com/mitchellh/cli"
)

func TestServerCommand_implement(t *testing.T) {
	var _ cli.Command = &ServerCommand{}
}
