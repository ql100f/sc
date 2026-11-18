package httpserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os/signal"
	"sc/internal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
)

const APIHTTPAddr = ":8080"

func init() {
	internal.Register(&httpServer2{})
}

type httpServer2 struct{}

func (h *httpServer2) Name() string {
	return "httpserver2"
}

func (h *httpServer2) Run(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	router := regestRoute()
	server := &http.Server{
		Addr:              APIHTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second, // 读取请求头超时控制，每次重新计时不影响第二次请求
		// ReadTimeout:       10 * time.Second, //比较鸡肋，限制一次建立TCP多次复用总时长，超过10s就关闭tcp 再重新建立tcp连接了
		WriteTimeout: 10 * time.Second, // 写超时控制，每次重新计时不影响第二次请求
		IdleTimeout:  90 * time.Second, // 空闲多久后关闭tcp连接
	}
	errCh := make(chan error, 1)
	go func() {
		fmt.Println("api listening", "address", APIHTTPAddr)
		errCh <- server.ListenAndServe()
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second) // 15s时间收尾没有执行完的请求
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func regestRoute() *gin.Engine {
	router := gin.New()
	_ = router.SetTrustedProxies(nil)
	router.Use(gin.Recovery(), gin.Logger()) // 加Recovery后，panic异常为【HTTP ERROR 500】，不加panic异常为【ERR_EMPTY_RESPONSE】
	router.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	return router
}
