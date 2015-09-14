package command

import (
	"testing"

	"github.com/mitchellh/cli"
)

func TestSessionCommand_implement(t *testing.T) {
	var _ cli.Command = &SessionCommand{}
}
