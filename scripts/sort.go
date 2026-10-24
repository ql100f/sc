package scripts

import (
	"context"
	"fmt"
	"sc/internal"
)

func init() {
	internal.Register(&msort{})
}

type msort struct{}

func (s *msort) Run(ctx context.Context) error {
	fmt.Println(bubbleSort("42724379657248345627542135"))
	return nil
}

func (s *msort) Name() string {
	return "sort"
}

func bubbleSort(str string) string {
	bstr := []byte(str)
	len := len(bstr)
	for j := 0; j < len-1; j++ {
		for i := 0; i < len-j-1; i++ {
			fmt.Println("j:", j, "i:", i)
			if bstr[i] > bstr[i+1] {
				tmp := bstr[i]
				bstr[i] = bstr[i+1]
				bstr[i+1] = tmp
			}
		}
	}

	return string(bstr)
}
