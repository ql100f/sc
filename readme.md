### 说明
脚本运行容器，可以自动注册新脚本，无需改动main.go等其它文件
sc : script container 脚本运行容器

main.go中一定要导入脚本目录
	_ "sc/demo"
	_ "sc/scripts"
用于在加载目录下所有文件的init()方法

### 其它
初始化项目 go mod init sc