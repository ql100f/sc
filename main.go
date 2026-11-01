package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	_ "sc/channel"
	_ "sc/demo"
	_ "sc/httpserver"
	"sc/internal"
	_ "sc/kafka"
	_ "sc/scripts"
)

var (
	sname string
	list  bool
)

func main() {
	flag.StringVar(&sname, "s", "demo", "请输入-s name 这样的格式")
	flag.BoolVar(&list, "list", false, "列出所有已注册脚本后退出")
	flag.Parse()

	if list {
		names := internal.List()
		fmt.Printf("共 %d 个脚本，用法：go run main.go -s <name>\n\n", len(names))
		for _, name := range names {
			fmt.Printf("  -s %s\n", name)
		}
		return
	}

	if sname == "" {
		os.Exit(-1)
	}

	task, ok := internal.Get(sname)
	if !ok {
		fmt.Printf("❌ 脚本%s，不在注册列表内%v", sname, internal.List())
		os.Exit(-1)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	err := internal.SafeRun(ctx, task)
	if err != nil {
		fmt.Printf("❌脚本执行异常，%v\n", err)
		os.Exit(0)
	}
	fmt.Println("🎉🎉🎉 脚本运行成功")
}
