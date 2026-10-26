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
	mux.HandleFunc("/", logger(home))
	mux.HandleFunc("/hello", hello)
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

func home(w http.ResponseWriter, r *http.Request) {
	// fmt.Fprint(w, "首页")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("首页"))
}

func hello(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("欢迎"))
}
