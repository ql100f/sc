package demo

import (
	"context"
	"log"
	"sc/internal"
)

func init() {
	internal.Register(&demoTask{})
}

type demoTask struct {
}

func (d *demoTask) Name() string {
	return "demo"
}

func (d *demoTask) Run(ctx context.Context) error {
	log.Println("demo")
	return nil
}
