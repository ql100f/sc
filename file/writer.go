package file

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sc/internal"
	"sort"
	"strings"

	"github.com/signintech/gopdf"
	"github.com/ygpkg/gooxml/document"
	"github.com/ygpkg/gooxml/presentation"
	"github.com/ygpkg/gooxml/spreadsheet"
)

func init() {
	internal.Register(&write{})
}

type write struct{}

func (w *write) Name() string {
	return "file-writer"
}

func (w *write) Run(ctx context.Context) error {
	entries, err := loadEntries()
	if err != nil {
		log.Fatalf("load entries: %v", err)
	}
	if len(entries) > perFormat {
		entries = entries[:perFormat]
	}
	log.Printf("loaded %d entries", len(entries))

	for _, e := range entries {
		writeCSV(e)
		writeHTML(e)
		writeDOCX(e)
		writePDF(e)
		writeXLSX(e)
		writePPTX(e)
	}
	log.Println("done ->", outRoot)
	return nil
}

type entry struct {
	ID      string
	Title   string
	Genre   string
	Source  string
	Content string
}

const (
	srcDir    = "file/datas/txt"
	outRoot   = "file/outputs"
	fontFile  = "C:\\Windows\\Fonts\\simhei.ttf"
	perFormat = 3 // 限制生产3个对应文件
)

// loadEntries 读取 novels 目录下 txt，按文件名排序取前 N 篇。
func loadEntries() ([]entry, error) {
	files, err := filepath.Glob(filepath.Join(srcDir, "*.txt"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)

	var list []entry
	for _, f := range files {
		fd, err := os.Open(f)
		if err != nil {
			fmt.Println("open file error:", err)
		}
		defer fd.Close() // todo 这个for循环里的defer不好，一个文件读完不会关闭文件，一直到所有都执行完了才会关闭，会占用很多fd，影响性能

		scanner := bufio.NewScanner(fd)
		buf := make([]byte, 1024*1024)
		scanner.Buffer(buf, 1024*1024)
		i := 0
		var title, body strings.Builder
		bodyStarted := false
		for scanner.Scan() {
			ln := scanner.Text()
			t := strings.TrimSpace(ln)
			if i == 0 && strings.HasPrefix(t, "# ") {
				title.WriteString(strings.TrimSpace(strings.TrimPrefix(t, "# ")))
				continue
			}
			if strings.HasPrefix(t, "## ") {
				continue
			}
			if t == "" {
				continue
			}
			if bodyStarted {
				body.WriteString("\n\n")
			}
			body.WriteString(t)
			bodyStarted = true
			i++
		}
		if err = scanner.Err(); err != nil {
			fmt.Println("read file line err")
		}

		base := strings.TrimSuffix(filepath.Base(f), ".txt")
		list = append(list, entry{
			ID:      base,
			Title:   title.String(),
			Genre:   "原创故事",
			Source:  filepath.Join("novels", filepath.Base(f)),
			Content: body.String(),
		})
	}
	return list, nil
}

func ensureDir(dir string) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Fatalf("mkdir %s: %v", dir, err)
	}
}

func outPath(format, name, ext string) string {
	dir := filepath.Join(outRoot, format)
	ensureDir(dir)
	return filepath.Join(dir, name+"."+ext)
}

// ---------- CSV ----------
func writeCSV(e entry) {
	p := outPath("csv", e.ID, "csv")
	f, err := os.Create(p)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	_ = w.Write([]string{"id", "title", "genre", "source", "content"})
	_ = w.Write([]string{e.ID, e.Title, e.Genre, e.Source, e.Content})
	w.Flush()
}

// ---------- HTML ----------
func writeHTML(e entry) {
	paras := strings.Split(e.Content, "\n\n")
	var b strings.Builder
	b.WriteString("<!DOCTYPE html>\n<html lang=\"zh-CN\">\n<head>\n<meta charset=\"utf-8\">\n")
	b.WriteString("<title>" + e.Title + "</title>\n</head>\n<body>\n")
	b.WriteString("<article>\n<h1>" + e.Title + "</h1>\n")
	b.WriteString("<p class=\"meta\">编号：" + e.ID + "　分类：" + e.Genre + "　来源：" + e.Source + "</p>\n")
	for _, p := range paras {
		b.WriteString("<p>" + strings.ReplaceAll(p, "\n", "<br>") + "</p>\n")
	}
	b.WriteString("</article>\n</body>\n</html>\n")
	if err := os.WriteFile(outPath("html", e.ID, "html"), []byte(b.String()), 0o644); err != nil {
		log.Fatal(err)
	}
}

// ---------- DOCX ----------
func writeDOCX(e entry) {
	doc := document.New()
	tp := doc.AddParagraph()
	tp.SetStyle("Title")
	tp.AddRun().AddText(e.Title)

	mp := doc.AddParagraph()
	mp.AddRun().AddText(fmt.Sprintf("编号：%s　分类：%s　来源：%s", e.ID, e.Genre, e.Source))

	for _, p := range strings.Split(e.Content, "\n\n") {
		para := doc.AddParagraph()
		para.AddRun().AddText(p)
	}
	if err := doc.SaveToFile(outPath("docx", e.ID, "docx")); err != nil {
		log.Fatal(err)
	}
}

// ---------- PDF ----------
func wrapCn(text string, max int) []string {
	var lines []string
	for _, para := range strings.Split(text, "\n\n") {
		var cur strings.Builder
		for _, r := range para {
			if cur.Len() >= max {
				lines = append(lines, cur.String())
				cur.Reset()
			}
			cur.WriteRune(r)
		}
		if cur.Len() > 0 {
			lines = append(lines, cur.String())
		}
	}
	return lines
}

func writePDF(e entry) {
	pdf := gopdf.GoPdf{}
	pdf.Start(gopdf.Config{PageSize: *gopdf.PageSizeA4})
	if err := pdf.AddTTFFont("cn", fontFile); err != nil {
		log.Fatal(err)
	}
	pdf.AddPage()
	_ = pdf.SetFont("cn", "", 16)
	pdf.SetXY(50, 60)
	_ = pdf.Text(e.Title)
	_ = pdf.SetFont("cn", "", 10)
	pdf.SetXY(50, 85)
	_ = pdf.Text(fmt.Sprintf("编号：%s　分类：%s　来源：%s", e.ID, e.Genre, e.Source))

	y := 115.0
	_ = pdf.SetFont("cn", "", 11)
	for _, ln := range wrapCn(e.Content, 30) {
		pdf.SetXY(50, y)
		_ = pdf.Text(ln)
		y += 18
		if y > 780 {
			pdf.AddPage()
			y = 60
		}
	}
	if err := pdf.WritePdf(outPath("pdf", e.ID, "pdf")); err != nil {
		log.Fatal(err)
	}
}

// ---------- XLSX ----------
func writeXLSX(e entry) {
	ss := spreadsheet.New()
	sheet := ss.AddSheet()
	header := []string{"id", "title", "genre", "source", "content"}
	row := sheet.AddRow()
	for _, h := range header {
		row.AddCell().SetString(h)
	}
	data := []string{e.ID, e.Title, e.Genre, e.Source, e.Content}
	row2 := sheet.AddRow()
	for _, v := range data {
		row2.AddCell().SetString(v)
	}
	if err := ss.SaveToFile(outPath("xlsx", e.ID, "xlsx")); err != nil {
		log.Fatal(err)
	}
}

// ---------- PPTX ----------
func writePPTX(e entry) {
	ppt := presentation.New()
	slide := ppt.AddSlide()

	titleBox := slide.AddTextBox()
	titleBox.Properties().SetPosition(50, 40)
	titleBox.Properties().SetWidth(800)
	tp := titleBox.AddParagraph()
	tr := tp.AddRun()
	tr.SetText(e.Title)
	tr.Properties().SetSize(28)

	bodyBox := slide.AddTextBox()
	bodyBox.Properties().SetPosition(50, 120)
	bodyBox.Properties().SetWidth(800)
	bodyBox.Properties().SetHeight(400)
	first := true
	for _, p := range strings.Split(e.Content, "\n\n") {
		bp := bodyBox.AddParagraph()
		br := bp.AddRun()
		br.SetText(p)
		br.Properties().SetSize(14)
		if first {
			first = false
		}
	}
	if err := ppt.SaveToFile(outPath("pptx", e.ID, "pptx")); err != nil {
		log.Fatal(err)
	}
}
