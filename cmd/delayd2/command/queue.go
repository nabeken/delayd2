package command

import (
	"strings"
)

type QueueCommand struct {
	Meta
}

func (c *QueueCommand) Run(args []string) int {
	// Write your code here

	return 0
}

func (c *QueueCommand) Synopsis() string {
	return ""
}

func (c *QueueCommand) Help() string {
	helpText := `

`
	return strings.TrimSpace(helpText)
}
