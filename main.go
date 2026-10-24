package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	_ "sc/demo"
	"sc/internal"
	_ "sc/scripts"
)

var sname string

func main() {
	flag.StringVar(&sname, "s", "demo", "请输入-d name 这样的格式")
	flag.Parse()

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
