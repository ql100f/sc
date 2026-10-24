package internal

import (
	"context"
	"fmt"
	"sync"
)

type Runner interface {
	Run(ctx context.Context) error
	Name() string
}

var funcMap = make(map[string]Runner)
var mu = sync.RWMutex{}

func Register(r Runner) {
	mu.Lock()
	defer mu.Unlock()
	funcMap[r.Name()] = r
}

func Get(name string) (Runner, bool) {
	mu.RLock()
	defer mu.RUnlock()
	r, ok := funcMap[name]
	return r, ok
}

func List() []string {
	mu.RLock()
	defer mu.RUnlock()
	var names = make([]string, 0, len(funcMap))
	for k := range funcMap {
		names = append(names, k)
	}
	return names
}

func SafeRun(ctx context.Context, r Runner) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic error : %v", p) // 此处是个闭包函数，err为外侧返回值中的err,panic时会改变这个err
		}
	}()
	return r.Run(ctx)
}
