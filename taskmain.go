package main

import (
	"log"
	"time"
)

func dowork(workid int) {

}

func main() {

	count := 102000
	max_open_count := 6616
	all_done := make(chan int, count)
	max_count := make(chan int, max_open_count)

	for i := 0; i < count; i++ {
		// log.Printf("begin try work,%v", i)
		go func(workid int) {
			all_done <- 1
			max_count <- 1
			time.Sleep(time.Second*1)
			// log.Printf("%v work done", workid)
			<-max_count
			<-all_done
		}(i)
	}

	for i := 0; i < count; i++ {
		all_done <- 1
		log.Printf("done work is %v/%v", i, count)
	}
	log.Printf("all count is done, count is %v", count)
}
