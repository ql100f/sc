package httpserver

import (
	"context"
	"fmt"
	"net/http"
	"sc/internal"
	"sync"
)

// 自己构造一个简易路由转发器，主要是思路

func init() {
	r := make(map[string]gog)
	h := &httpServer3{
		routers: r,
	}
	internal.Register(h)
}

type httpServer3 struct {
	mu      sync.Mutex
	routers map[string]gog
}

func (h *httpServer3) Get(url string, g gog) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.routers[url] = g
}

func (h *httpServer3) Name() string {
	return "httpserver3"
}

func (h *httpServer3) Run(ctx context.Context) error {
	fmt.Println("---")
	h.Get("/home", home2)
	h.Get("/hei", hei)
	err := http.ListenAndServe(":8081", h)
	if err != nil {
		fmt.Println("erros", err)
	}
	return nil
}

func (h *httpServer3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	for p, g := range h.routers {
		if r.URL.Path == p && r.Method == "GET" {
			g("url:" + p)
		}
	}

}

type gog func(string)

func home2(name string) {
	fmt.Println(name)
}
func hei(name string) {
	fmt.Println(name)
}
