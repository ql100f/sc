package channel

import (
	"context"
	"sc/internal"
)

func init() {
	internal.Register(&channel{})
}

type channel struct {
}

func (c *channel) Name() string {
	return "channel"
}

func (c *channel) Run(ctx context.Context) error {
	// Run_a()
	// Run_b()
	Run_c()
	return nil
}
