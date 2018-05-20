package yxsdb

import (
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

func addNodeKGroup(node *DataNode) {
	kgroup1 := InitKafkaGroup("localhost:9092")
	node.KafkaGroup = kgroup1
	consumer := kgroup1.GetConsumer("node")
	requestHandler := func(msg *sarama.ConsumerMessage, group *KafkaGroup) {
		log.Printf("Node=%v get message, key=%s   value=%s   offset=%v, topic=%s", node.ID, msg.Key, msg.Value, msg.Offset, msg.Topic)
		msgtype := msg.Key
		if strings.Compare(string(msgtype), "execute") == 0 {
			log.Printf("DO execute")
			tableName := 4030
			ret := node.ExecuteNodeTable(tableName, string(msg.Value))
			if ret.err != nil {
				log.Printf("ret is %v", ret)
				// return
			}
			log.Printf("query ret is %v", ret)
			producer, _ := kgroup1.GetProducer("response")
			kgroup1.SendMessage(producer, "response", "execute", fmt.Sprintf("%v", ret))
		}

		if strings.Compare(string(msgtype), "query") == 0 {
			log.Printf("DO query")
			tableName := 4030
			ret := node.QueryNodeTable(tableName, string(msg.Value), string(msg.Value))
			if ret.err != nil {
				log.Printf("ret is %v", ret)
				// return
			}
			ret.CacheTable.RowsShowCount = 10
			log.Printf("query ret is %v", ret)
			producer, _ := kgroup1.GetProducer("response")
			kgroup1.SendMessage(producer, "response", "query", fmt.Sprintf("node=%v,ret=%v,request=%s", node.ID, ret, msg.Value))
		}

	}

	kgroup1.HandleMessage(consumer, "request", 0, requestHandler)

}

func addMasterNodeKGroup(node *DataNode) {
	kgroup1 := InitKafkaGroup("localhost:9092")
	node.KafkaGroup = kgroup1
	consumer := kgroup1.GetConsumer("master")

	responseHandler := func(msg *sarama.ConsumerMessage, group *KafkaGroup) {
		log.Printf("Node=%v get message, key=%s   value=%s   offset=%v, topic=%s", node.ID, msg.Key, msg.Value, msg.Offset, msg.Topic)
	}

	kgroup1.HandleMessage(consumer, "response", 0, responseHandler)

}

func TestKafkaDB(t *testing.T) {
	masterNode := InitNode(0, "/dev/shm/target/data", 3)
	node1 := InitNode(1, "/dev/shm/target/data", 3)
	node2 := InitNode(2, "/dev/shm/target/data", 3)
	node3 := InitNode(3, "/dev/shm/target/data", 3)

	addMasterNodeKGroup(masterNode)
	addNodeKGroup(node1)
	addNodeKGroup(node2)
	addNodeKGroup(node3)

	masterNode.KafkaGroup.CreateTopic("requeset")
	masterNode.KafkaGroup.CreateTopic("response")

	producer, _ := masterNode.KafkaGroup.GetProducer("request")
	masterNode.KafkaGroup.SendMessage(producer, "request", "execute", "create table if not exists _0._4030(_0,_1,_2);")
	masterNode.KafkaGroup.SendMessage(producer, "request", "execute", "insert into _0._4030 select 1,2,3;")

	masterNode.KafkaGroup.SendMessage(producer, "request", "query", "select * from _0._4030;")

	time.Sleep(10 * time.Second)
	log.Printf("end")
}
