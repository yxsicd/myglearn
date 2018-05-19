package yxsdb

import (
	"log"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

func TestKafka(t *testing.T) {
	log.Printf("begin")

	kgroup := InitKafkaGroup("localhost:9092")

	maxLevel1 := 3
	maxLevel2 := 5
	kgroup.BatchTopicCreate(3, 5)
	kgroup.CreateTopic("dbmeta")
	kgroup.CreateTopic("nodemeta")

	kgroup.AddAllNode(maxLevel1, maxLevel2)

	consumer := kgroup.GetConsumer("master")
	masterHandler := func(msg *sarama.ConsumerMessage, group *KafkaGroup) {
		log.Printf("MASTER Node get message, %s=%s, offset=%v, topic=%s", msg.Key, msg.Value, msg.Offset, msg.Topic)
	}
	kgroup.HandleMessage(consumer, kgroup.GetSubTopicName("response", maxLevel1, maxLevel2), 0, masterHandler)

	masterP, _ := kgroup.GetProducer("master")
	log.Printf("masterP is %v", masterP)

	for i := 0; i < 1; i++ {
		kgroup.SendMessage(masterP, kgroup.GetNodeTopicNameByID("request", int64(i), maxLevel1, maxLevel2), "1", "create table if not exists _0(_0,_1,_2,_3)")
		time.Sleep(50 * time.Millisecond)
	}

	for i := 0; i < 1; i++ {
		kgroup.SendMessage(masterP, kgroup.GetSubTopicName("request", maxLevel1, maxLevel2), "1", "create table if not exists _0(_0,_1,_2,_3)")
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(2 * time.Second)
	log.Printf("end")
}
