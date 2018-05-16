package yxsdb

import (
	"log"
	"testing"
	"time"

	"github.com/Shopify/sarama"
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

	consumer := getConsumer("master")
	masterHandler := func(msg *sarama.ConsumerMessage) {
		log.Printf("MASTER Node get message, %s=%s, offset=%v, topic=%s", msg.Key, msg.Value, msg.Offset, msg.Topic)
	}
	handleMessage(consumer, getSubTopicName("response", maxLevel1, maxLevel2), masterHandler)

	masterP, _ := getProducer("master")
	log.Printf("masterP is %v", masterP)

	for i := 0; i < 1; i++ {
		sendMessage(masterP, getNodeTopicNameByID("request", int64(i), maxLevel1, maxLevel2), "1", "create table if not exists _0(_0,_1,_2,_3)")
		time.Sleep(50 * time.Millisecond)
	}

	for i := 0; i < 1; i++ {
		sendMessage(masterP, getSubTopicName("request", maxLevel1, maxLevel2), "1", "create table if not exists _0(_0,_1,_2,_3)")
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(10 * time.Second)
	log.Printf("end")
}
