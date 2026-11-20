// Package extract 从多种文档格式中提取纯文本，供 RAG 切片、向量化、入库使用。
// 设计为流式：ExtractStream 逐段回调文本，不把整篇文档聚合到一个巨型字符串，
// 以控制大文件（百 MB ~ GB 级）下的内存占用；切片/向量化/入库复用 internal/rag。
package file

import (
	"archive/zip"
	"bufio"
	"encoding/csv"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	pdf "github.com/ledongthuc/pdf"
	"github.com/ygpkg/gooxml/spreadsheet"
	"golang.org/x/net/html"
)

// supported 记录受支持的扩展名（小写、含点）。
var supported = map[string]bool{
	".csv": true, ".html": true, ".htm": true, ".docx": true,
	".pdf": true, ".xlsx": true, ".pptx": true, ".xml": true, ".txt": true,
}

// Supported 判断文件扩展名是否受支持。
func Supported(path string) bool {
	return supported[strings.ToLower(filepath.Ext(path))]
}

// ChunkSink 接收一段提取出的正文；返回非 nil 即中止提取。
type ChunkSink func(text string) error

// ExtractStream 按扩展名流式提取，每产出一段可独立入库的文本段就回调 sink。
// 实现侧逐行/逐页/逐段/逐幻灯片读取，不在内存中保留整篇文档。
func ExtractStream(path string, sink ChunkSink) error {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".txt":
		return readTxtStream(path, sink)
	case ".csv":
		return readCSVStream(path, sink)
	case ".html", ".htm":
		return readHTMLStream(path, sink)
	case ".docx":
		return readDOCXStream(path, sink)
	case ".pdf":
		return readPDFStream(path, sink)
	case ".xlsx":
		return readXLSXStream(path, sink)
	case ".pptx":
		return readPPTXStream(path, sink)
	case ".xml":
		return readXMLStream(path, sink)

	default:
		return fmt.Errorf("unsupported format: %s", filepath.Ext(path))
	}
}

// ExtractFile 兼容旧接口：把整篇文本聚合为一个字符串返回（适合小文件与测试）。
// 大文件请优先使用 ExtractStream 逐段处理。
func ExtractFile(path string) (string, error) {
	var b strings.Builder
	err := ExtractStream(path, func(text string) error {
		b.WriteString(text)
		b.WriteString("\n")
		return nil
	})
	return strings.TrimSpace(b.String()), err
}

// ---------- CSV（逐行流式） ----------

func readCSVStream(path string, sink ChunkSink) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1 // 允许每行字段数不一致
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if line := strings.TrimSpace(strings.Join(rec, " | ")); line != "" {
			if err := sink(line); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---------- HTML（Tokenizer 流式，跳过 script/style） ----------

// htmlBlockTags 为块级标签，遇到即先冲刷当前缓冲的文本段。
var htmlBlockTags = map[string]bool{
	"p": true, "div": true, "article": true, "section": true,
	"h1": true, "h2": true, "h3": true, "h4": true, "h5": true, "h6": true,
	"li": true, "tr": true, "br": true, "header": true, "footer": true,
}

// htmlFlushLimit 为单段缓冲阈值，超过即先冲刷，避免缓冲无界增长。
const htmlFlushLimit = 4096

func readHTMLStream(path string, sink ChunkSink) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	tok := html.NewTokenizer(f)
	var skipDepth int
	var buf strings.Builder

	flush := func() error {
		t := strings.TrimSpace(buf.String())
		buf.Reset()
		if t != "" {
			return sink(t)
		}
		return nil
	}

	for {
		tt := tok.Next()
		if tt == html.ErrorToken { // 含 io.EOF
			break
		}
		switch tt {
		case html.StartTagToken, html.SelfClosingTagToken:
			name, _ := tok.TagName()
			n := string(name)
			if n == "script" || n == "style" {
				skipDepth++
				continue
			}
			if htmlBlockTags[n] {
				if err := flush(); err != nil {
					return err
				}
			}
		case html.EndTagToken:
			name, _ := tok.TagName()
			n := string(name)
			if n == "script" || n == "style" {
				if skipDepth > 0 {
					skipDepth--
				}
				continue
			}
			if htmlBlockTags[n] {
				if err := flush(); err != nil {
					return err
				}
			}
		case html.TextToken:
			if skipDepth > 0 {
				continue
			}
			t := strings.TrimSpace(string(tok.Text()))
			if t == "" {
				continue
			}
			buf.WriteString(t)
			buf.WriteString(" ")
			if buf.Len() >= htmlFlushLimit {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
	return flush()
}

// ---------- DOCX（zip + XML 流式，按段落 w:p 收集 w:t） ----------

func readDOCXStream(path string, sink ChunkSink) error {
	zr, err := zip.OpenReader(path)
	if err != nil {
		return err
	}
	defer zr.Close()

	for _, zf := range zr.File {
		if zf.Name != "word/document.xml" {
			continue
		}
		rc, err := zf.Open()
		if err != nil {
			continue
		}
		err = streamParaXML(rc, "p", "t", sink)
		rc.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// ---------- PPTX（zip 逐幻灯片，按 a:p 收集 a:t） ----------

func readPPTXStream(path string, sink ChunkSink) error {
	zr, err := zip.OpenReader(path)
	if err != nil {
		return err
	}
	defer zr.Close()

	for _, zf := range zr.File {
		if !strings.HasPrefix(zf.Name, "ppt/slides/slide") ||
			!strings.HasSuffix(zf.Name, ".xml") {
			continue
		}
		rc, err := zf.Open()
		if err != nil {
			continue
		}
		err = streamParaXML(rc, "p", "t", sink)
		rc.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// paraCollector 按段落标签收集文本，段落闭合时回调一段。
// 前缀无关，仅按 local-name 匹配，兼容 w:p/w:t 与 a:p/a:t。
type paraCollector struct {
	paraLocal, textLocal string
	inPara, inText       bool
	paraBuf, textBuf     strings.Builder
	sink                 ChunkSink
}

func (c *paraCollector) start(local string) {
	if local == c.paraLocal {
		c.inPara = true
		c.paraBuf.Reset()
	}
	if local == c.textLocal {
		c.inText = true
		c.textBuf.Reset()
	}
}

func (c *paraCollector) charData(b []byte) {
	if c.inText {
		c.textBuf.Write(b)
	}
}

func (c *paraCollector) end(local string) error {
	if local == c.textLocal {
		c.inText = false
		c.paraBuf.WriteString(strings.TrimSpace(c.textBuf.String()))
	}
	if local == c.paraLocal {
		c.inPara = false
		if t := strings.TrimSpace(c.paraBuf.String()); t != "" {
			return c.sink(t)
		}
	}
	return nil
}

// streamParaXML 用 xml.Decoder 逐 token 扫描，段落闭合回调一段，不整树载入。
func streamParaXML(r io.Reader, paraLocal, textLocal string, sink ChunkSink) error {
	dec := xml.NewDecoder(r)
	c := &paraCollector{paraLocal: paraLocal, textLocal: textLocal, sink: sink}
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			c.start(t.Name.Local)
		case xml.CharData:
			c.charData(t)
		case xml.EndElement:
			if err := c.end(t.Name.Local); err != nil {
				return err
			}
		}
	}
}

// ---------- PDF（逐页流式） ----------

func readPDFStream(path string, sink ChunkSink) error {
	f, r, err := pdf.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	for i := 1; i <= r.NumPage(); i++ {
		p := r.Page(i)
		text, err := p.GetPlainText(nil) // nil 时库内按页自建字体缓存
		if err != nil {
			continue
		}
		if t := strings.TrimSpace(text); t != "" {
			if err := sink(t); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---------- XLSX（封装逐行回调；整簿由库载入，超大表受库内存限制） ----------

func readXLSXStream(path string, sink ChunkSink) error {
	wb, err := spreadsheet.Open(path)
	if err != nil {
		return err
	}
	defer wb.Close()

	for _, sh := range wb.Sheets() {
		for _, row := range sh.Rows() {
			var cells []string
			for _, c := range row.Cells() {
				if v := strings.TrimSpace(c.GetString()); v != "" {
					cells = append(cells, v)
				}
			}
			if len(cells) > 0 {
				if err := sink(strings.Join(cells, " | ")); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// ---------- XML（逐元素流式，顶层元素结束回调一段） ----------

func readXMLStream(path string, sink ChunkSink) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	dec := xml.NewDecoder(f)
	var depth int
	var buf strings.Builder

	flush := func() error {
		t := strings.TrimSpace(buf.String())
		buf.Reset()
		if t != "" {
			return sink(t)
		}
		return nil
	}

	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			depth++
		case xml.CharData:
			if s := strings.TrimSpace(string(t)); s != "" {
				buf.WriteString(s)
				buf.WriteString(" ")
			}
		case xml.EndElement:
			depth--
			if depth <= 0 {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
	return flush()
}

// ---------- TXT（逐行流式） ----------

func readTxtStream(path string, sink ChunkSink) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	// 加大行缓冲，避免超长行触发 Scanner 错误。
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); line != "" {
			if err := sink(line); err != nil {
				return err
			}
		}
	}
	return sc.Err()
}

// 不是逐行处理，而是按段聚合成段再处理，适合流式读取大文件按段输出的场景，比如rag切片
func readTxtStreamRecursive(path string, sink ChunkSink) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	// 加大行缓冲，避免超长行触发 Scanner 错误。
	sc.Buffer(make([]byte, 64*1024), 1024*1024)

	var para []string
	flush := func() error {
		if len(para) == 0 {
			return nil
		}
		text := strings.Join(para, " ")
		para = nil
		if err := sink(text); err != nil {
			return err
		}
	}

	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			// 空行作为段落边界，冲刷当前段落。
			if err := flush(); err != nil {
				return err
			}
			continue
		}
		para = append(para, line)
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return flush()
}
