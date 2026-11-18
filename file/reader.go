package file

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sc/internal"
)

func init() {
	internal.Register(&reader{})
}

type reader struct{}

func (w *reader) Name() string {
	return "file-reader"
}

func (w *reader) Run(ctx context.Context) error {
	dir := "file/datas"
	var files []string
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !d.IsDir() && Supported(path) {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		log.Fatalf("walk docs dir: %v", err)
	}

	for _, path := range files {
		// source 用相对路径，便于区分格式子目录。
		rel, _ := filepath.Rel(dir, path)
		rel = filepath.ToSlash(rel)

		// 流式逐段入库：整篇文本不驻留内存，单段失败不中断整文件。
		err := ExtractStream(path, func(seg string) error {
			log.Println("content: ", seg) // TODO 这个打印只是示例，可以操作其它的，比如入库、入队列等
			return nil
		})
		if err != nil {
			log.Printf("解析失败 %s: %v", rel, err)
			continue
		}
	}
	log.Printf("完成：本次读取")
	return nil
}
