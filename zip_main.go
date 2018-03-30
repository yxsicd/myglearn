package main

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"runtime"
	"strconv"
	"strings"
)

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func wzip() {
	// Create a buffer to write our archive to.
	buf := new(bytes.Buffer)
	log.Printf("write file begin")
	PrintMemUsage()
	// Create a new zip archive.
	w := zip.NewWriter(buf)

	// Add some files to the archive.
	var files = []struct {
		Name, Body string
	}{
		{"readme.txt", "This archive contains some text files."},
		{"gopher.txt", "Gopher names:\nGeorge\nGeoffrey\nGonzo"},
		{"todo.txt", "Get animal handling licence.\nWrite more examples."},
	}
	nfile := files
	for i := 0; i < 100; i++ {
		nfile = append(nfile, struct{ Name, Body string }{strconv.Itoa(i), strings.Repeat(strconv.Itoa(i), 100*20000)})
	}
	for _, file := range nfile {
		f, err := w.Create(file.Name)
		if err != nil {
			log.Fatal(err)
		}
		_, err = f.Write([]byte(file.Body))
		if err != nil {
			log.Fatal(err)
		}
	}

	// log.Printf("zip w is %v", w)

	// Make sure to check the error on Close.
	err := w.Close()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("write file done, zip file size is %v", buf.Len())
	PrintMemUsage()

	// Open a zip archive for reading.
	r, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		log.Fatal(err)
	}

	// Iterate through the files in the archive,
	// printing some of their contents.
	sum := 0
	size := 0
	for _, f := range r.File {
		sum += 1
		// log.Printf("----------------%s:---------------------\n", f.Name)
		rc, err := f.Open()
		if err != nil {
			log.Fatal(err)
		}
		defer rc.Close()
		content, err := ioutil.ReadAll(rc)
		if err != nil {
			log.Fatal(err)
		}
		size += len(content)
		// log.Printf("%s", content)
	}
	log.Printf("file count is %v, all size is %v", sum, size)
	PrintMemUsage()
}

func main() {
	wzip()
}
