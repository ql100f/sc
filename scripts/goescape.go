// goroutine泄露演示
package scripts

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"sc/internal"
	"sync"
	"time"
)

func ticker(ctx context.Context, f func(), d time.Duration) {
	ticker := time.NewTicker(d)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			go f()
		case <-ctx.Done():
			fmt.Println("ticker stop ,ctx done")
			return
		}
	}
}

func mutexTest() {
	m := sync.Mutex{}
	for i := 0; i < 1000; i++ {
		go func() {
			m.Lock()
		}()
	}

	time.Sleep(5 * time.Second) // 5s后 go mutexTest() 这个协程会退出，注册输入结果在5s后会-1
}

func init() {
	internal.Register(&goescape{})

}

type goescape struct{}

func (g *goescape) Run(ctx context.Context) error {
	// 增加一个net/http/pprof的分析 ，注意一定要导入 _ "net/http/pprof"包
	// 可直接访问 http://localhost:6060/debug/pprof/
	// 另外还可以通过go tool pprof -http=:8081 http://localhost:6060/debug/pprof/goroutine 来查看火焰图

	go func() {
		http.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, "hello world")
		})
		http.ListenAndServe(":6060", nil)
	}()

	go ticker(ctx, func() {
		fmt.Println("goroutine:", runtime.NumGoroutine())
	}, time.Second)
	fmt.Println("----------------")
	go mutexTest()
	time.Sleep(time.Second * 200)
	return nil
}

func (g *goescape) Name() string {
	return "goescape"
}
