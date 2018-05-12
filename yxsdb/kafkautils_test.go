package yxsdb

import (
	"log"
	"testing"
)

func TestKafka(t *testing.T) {
	log.Printf("begin")

	maxLevel1 := 3
	maxLevel2 := 5
	doTopicCreate(3, 5)
	createTopic("dbmeta")
	createTopic("nodemeta")

	// doProducer()
	// doConsumer()
	addAllNode(maxLevel1, maxLevel2)

	masterP, _ := getProducer("master")
	log.Printf("masterP is %v", masterP)

	for i := 0; i < 10; i++ {
		sendMessage(masterP, getTopicName(int64(i), maxLevel1, maxLevel2), "1", "create table _0(_0,_1,_2,_3)")
		// time.Sleep(1 * time.Millisecond)
	}
	log.Printf("end")
}
