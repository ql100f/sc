package httpserver

import (
	"context"
	"log"
	"net/http"
	"sc/internal"
)

func init() {
	internal.Register(&httpServer{})
}

type httpServer struct{}

func (h *httpServer) Name() string {
	return "httpserver"
}

func (h *httpServer) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	limit := LimitMiddleware(3) // 最大并发3

	mux.HandleFunc("/", logger(home))
	mux.Handle("/hello", limit(http.HandlerFunc(hello)))
	http.ListenAndServe(":8080", mux)
	return nil
}

func logger(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Println("i am coming")
		h(w, r)
		log.Println("i am going")
	}
}

// LimitMiddleware 并发限流中间件
func LimitMiddleware(maxConcurrency int) func(http.Handler) http.Handler {
	sem := make(chan struct{}, maxConcurrency)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case sem <- struct{}{}:
				defer func() {
					<-sem
				}()
				next.ServeHTTP(w, r)
			default:
				// 无空闲令牌，直接拒绝（不排队）
				http.Error(w, " too many request, limit", http.StatusTooManyRequests)
				return
			}
		})
	}
}

func home(w http.ResponseWriter, r *http.Request) {
	// fmt.Fprint(w, "首页")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("首页"))
}

func hello(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("欢迎"))
}
