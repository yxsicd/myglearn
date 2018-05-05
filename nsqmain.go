package main

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"time"
)

func main() {

	count := 10000
	msgUrl := "http://127.0.0.1:4151/pub?topic=test"
	contentType := "text/plain"
	sendCount := 0
	beginTime := time.Now()
	for i := 0; i < count; i++ {
		http.Post(msgUrl, contentType, bytes.NewReader([]byte(fmt.Sprintf("value-%v", i))))
		sendCount++
	}
	log.Printf("user time is %v,count is %v", time.Since(beginTime), sendCount)

}
