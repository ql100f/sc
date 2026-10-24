package scripts

import (
	"context"
	"fmt"
	"sc/internal"
	"sync"
)

func init() {
	internal.Register(&ring{})
}

type ring struct{}

func (r *ring) Name() string {
	return "ring"
}

func (r *ring) Run(ctx context.Context) error {

	rs := NewRingStat(5)

	rs.Add(1)
	rs.Add(2)
	rs.Add(3)
	rs.Add(4)
	rs.Add(5)
	rs.Add(6)
	fmt.Println(rs)
	fmt.Println(rs.Len(), rs.buf, rs.sum, rs.count, rs.Avg())
	return nil
}

type ringStat struct {
	mu    sync.RWMutex
	buf   []int //数组队列
	cap   int   // 容量
	count int   // 已存数量
	sum   int   // 总数
	key   int   //当前要写入的键
}

func NewRingStat(cap int) *ringStat {
	return &ringStat{
		cap: cap,
		buf: make([]int, cap),
		key: 0,
	}
}

func (r *ringStat) Add(val int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.count >= r.cap {
		r.sum -= r.buf[r.key]
	} else {
		r.count++
	}

	r.buf[r.key] = val
	r.sum += val

	r.key = (r.key + 1) % r.cap
}

func (r *ringStat) Avg() float32 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.count == 0 {
		return 0
	}
	return float32(r.sum) / float32(r.count)
}

func (r *ringStat) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.count
}
