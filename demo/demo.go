package demo

import (
	"context"
	"fmt"
	"log"
	"sc/internal"
	"time"
)

func init() {
	internal.Register(&demoTask{})
}

type demoTask struct {
}

func (d *demoTask) Name() string {
	return "demo"
}

// 泛型应用 ，方法名后在[]内定义泛型类型,T不是固定的，可以是K Y等等
func exists[T int | string](t []T, i T) string {
	for _, v := range t {
		if v == i {
			return "existed"
		}
	}
	return "not existed"
}

func printBit() {
	var str string = "hello"
	bstr := []byte(str)
	log.Printf("拷贝版本 bitsCopy: %08b", bstr)
}

func (d *demoTask) Run(ctx context.Context) error {
	log.Println("demo")
	// 装饰器模式，http中间件都是此用法
	decorator()

	// 监听执行完毕及中断操作
	// listenWork(ctx)

	// cpu核数，与GOMAXPROCS对应
	// log.Println(runtime.NumCPU())

	// 打印bit
	// printBit()

	// 泛型应用
	// log.Println("string 判断：", exists([]string{"aa", "bb"}, "cc"))
	// log.Println("int 判断：", exists([]int{1, 3, 5, 7, 8}, 7))
	return nil
}

func listenWork(ctx context.Context) {
	done := make(chan struct{})
	go streamAgent(ctx, done)
	<-done
	log.Println("over")
}

func streamAgent(ctx context.Context, done chan struct{}) error {
	// 模拟LLM流式返回chan
	streamCh := make(chan string, 4)
	go func() {
		defer close(streamCh)
		for _, chunk := range []string{"你好", "我是Agent", "正在思考", "完成"} {
			select {
			case <-ctx.Done():
				return
			case streamCh <- chunk:
			}
			time.Sleep(600 * time.Millisecond)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("流式任务被中断")
			return ctx.Err()
		case chunk, ok := <-streamCh:
			if !ok {
				fmt.Println("流式输出完成")
				done <- struct{}{}
				return nil
			}
			fmt.Printf("输出chunk：%s\n", chunk)
		}
	}

}

func decorator() {
	origin := func(msg string) {
		time.Sleep(200 * time.Millisecond)
		fmt.Println("stream output:", msg)
	}
	// 链式包装
	wrapped := WithLogDecorator(WithTimeDecorator(origin))
	wrapped("hello agent")
}

// 原始函数类型
type StreamFunc func(msg string)

// 耗时装饰器
func WithTimeDecorator(f StreamFunc) StreamFunc {
	return func(msg string) {
		start := time.Now()
		f(msg)
		fmt.Printf("执行耗时：%v\n", time.Since(start))
	}
}

// 日志装饰器
func WithLogDecorator(f StreamFunc) StreamFunc {
	return func(msg string) {
		fmt.Printf("[LOG] 入参:%s\n", msg)
		f(msg)
	}
}
