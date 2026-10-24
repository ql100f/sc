package demo

import (
	"context"
	"fmt"
	"sc/internal"
)

func init() {
	internal.Register(&DemoTask{})
}

type DemoTask struct {
}

func (d *DemoTask) Name() string {
	return "demo"
}

func (d *DemoTask) Run(ctx context.Context) error {
	fmt.Println("demo")
	return nil
}
