### 说明
脚本运行容器，可以自动注册新脚本，无需改动main.go等其它文件

sc : script container 脚本运行容器

main.go中一定要导入脚本目录  
	_ "sc/demo"  
	_ "sc/scripts"  
用于在加载目录下所有文件的init()方法


### 运行示例

查看所有已注册脚本（按名称排序）：
	go run main.go -list

运行指定脚本：
	go run main.go -s demo
	go run main.go -s ring

### VS Code 调试
.vscode/launch.json 已内置配置，按 F5 选「运行 sc 脚本（输入名称）」，
输入脚本名即可带断点启动；也可以选现成的 channel / httpserver / kafka 等配置。

### 其它
初始化项目 go mod init sc  
