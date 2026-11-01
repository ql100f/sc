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
	// decorator()

	// 策略模式：同一套流程，运行时替换算法
	strategyDemo()

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

// ==================== 策略模式 ====================
//
// 和上面的装饰器对比着看：
//   装饰器：调用方不变，给同一个函数纵向“套”上额外行为（增强）
//   策略：  流程不变，把可变的算法抽成接口，运行时横向“换”掉（选择）
// 两者都靠接口解耦，区别只在意图。

// TrimStrategy 是策略接口：只描述“裁剪上下文”这一个能力
type TrimStrategy interface {
	Name() string
	Trim(history []string, keep int) []string
}

// recentTrim：只保留最近 keep 条，滑动窗口
type recentTrim struct{}

func (recentTrim) Name() string { return "recent" }

func (recentTrim) Trim(history []string, keep int) []string {
	if len(history) <= keep {
		return history
	}
	// 注意：返回的是原切片的子切片，与入参共享底层数组，只读场景才安全
	return history[len(history)-keep:]
}

// headTailTrim：首尾各留一半，保住开场的 system 设定 + 最新对话
type headTailTrim struct{}

func (headTailTrim) Name() string { return "head-tail" }

func (headTailTrim) Trim(history []string, keep int) []string {
	if len(history) <= keep {
		return history
	}
	head := keep / 2
	tail := keep - head
	out := make([]string, 0, keep)
	out = append(out, history[:head]...)
	out = append(out, history[len(history)-tail:]...)
	return out
}

// TrimFunc 是 Go 的惯用法：给函数类型定义方法，它就直接满足接口，
// 不必为每个策略都写一个结构体（标准库的 http.HandlerFunc 就是这么干的）
type TrimFunc func(history []string, keep int) []string

func (f TrimFunc) Name() string { return "func" }

func (f TrimFunc) Trim(history []string, keep int) []string { return f(history, keep) }

// historyWindow 是上下文角色：持有策略，对外只暴露接口方法
type historyWindow struct {
	strategy TrimStrategy
	history  []string
	keep     int
}

func (h *historyWindow) SetStrategy(s TrimStrategy) { h.strategy = s }

func (h *historyWindow) Build() []string { return h.strategy.Trim(h.history, h.keep) }

func strategyDemo() {
	h := &historyWindow{
		strategy: recentTrim{}, // 默认策略
		keep:     4,
		history: []string{
			"system: 你是代码助手",
			"user: 你好",
			"assistant: 你好，有什么可以帮你",
			"user: 讲下 goroutine",
			"assistant: goroutine 是 Go 的轻量协程",
			"user: 那 channel 呢",
			"assistant: channel 用于协程间通信",
			"user: 谢谢",
		},
	}

	// 业务代码只认 TrimStrategy 接口，换算法不用改这里
	for _, s := range []TrimStrategy{
		recentTrim{},
		headTailTrim{},
		// first-N：保住开场设定，直接用一个函数当策略，省掉结构体
		TrimFunc(func(history []string, keep int) []string { return history[:keep] }),
	} {
		h.SetStrategy(s)
		out := h.Build()
		fmt.Printf("策略=%-9s 保留 %d/%d 条 → %v\n", s.Name(), len(out), len(h.history), out)
	}
}
