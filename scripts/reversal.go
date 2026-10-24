package scripts

import (
	"context"
	"fmt"
	"sc/internal"
)

func init() {
	internal.Register(&char{})
}

type char struct{}

func (c *char) Run(ctx context.Context) error {
	fmt.Println(reversal("char"))
	return nil
}

func (c *char) Name() string {
	return "char"
}

func reversal(str string) string {
	bstr := []byte(str)
	len := len(bstr)

	for i := 0; i < len/2; i++ {
		bstr[i], bstr[len-i-1] = bstr[len-i-1], bstr[i]
	}
	return string(bstr)
}
