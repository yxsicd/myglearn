package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {
	msgq := make(chan string, 10000000)
	donemsg := make(chan string, 1)

	go (func() {
		for true {
			msg := <-msgq
			log.Printf("received message %s", msg)
		}
	})()

	go (func() {
		reader := bufio.NewReader(os.Stdin)
		for true {
			line, _ := reader.ReadByte()
			msgq <- string(line)
			// log.Printf("receive string is %s", line)
			if strings.Compare(string(line), "q") == 0 {
				log.Printf("exit !!!!")
				donemsg <- "done"
			}
		}
	})()

	for i := 0; i < 100; i++ {
		go (func(m string) {
			msgq <- m
		})(fmt.Sprintf("%v", i))
	}
	log.Printf("log is good")

	<-donemsg
}
