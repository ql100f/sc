package scripts

import (
	"context"
	"fmt"
	"sc/internal"
	"sync"
	"time"
)

func init() {
	internal.Register(&Bucket{})
}

type Bucket struct{}

func (b *Bucket) Name() string {
	return "bucket"
}
func (b *Bucket) Run(ctx context.Context) error {
	bucketCheck()
	return nil
}

type TokenBucket struct {
	m            sync.Mutex
	maxToken     float64
	currentToken float64
	lastTime     time.Time
	rate         float64
}

func NewTokenBucket(maxToken, rate float64) *TokenBucket {
	return &TokenBucket{
		maxToken:     maxToken,
		rate:         rate,
		lastTime:     time.Now(),
		currentToken: maxToken,
	}
}

func (tb *TokenBucket) isAllow() bool {
	tb.m.Lock()
	defer tb.m.Unlock()

	// 计算当前可用token
	t := time.Now()
	subTime := t.Sub(tb.lastTime).Seconds()
	fmt.Printf("subTime:%f - ", subTime)
	newToken := subTime * tb.rate
	currentToken := tb.currentToken + newToken
	if currentToken > tb.maxToken {
		tb.currentToken = tb.maxToken
	} else {
		tb.currentToken = currentToken
	}
	tb.lastTime = time.Now()
	if tb.currentToken > 0 {
		fmt.Printf("%f - ", tb.currentToken)
		tb.currentToken--
		return true
	}
	return false
}

func bucketCheck() {
	tb := NewTokenBucket(8, 1)
	for i := 0; i < 10; i++ {
		if tb.isAllow() {
			fmt.Printf("i:%d is allowed\n", i)
		} else {
			fmt.Printf("i:%d is rejected\n", i)
		}
	}
}
