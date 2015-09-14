package command

import (
	"strings"
)

type SessionCommand struct {
	Meta
}

func (c *SessionCommand) Run(args []string) int {
	// Write your code here

	return 0
}

func (c *SessionCommand) Synopsis() string {
	return ""
}

func (c *SessionCommand) Help() string {
	helpText := `

`
	return strings.TrimSpace(helpText)
}
