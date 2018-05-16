package yxsdb

import (
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

var (
	producerMap     = make(map[string]*sarama.AsyncProducer)
	producerMapLock = make(chan bool, 1)

	consumerMap     = make(map[string]*sarama.Consumer)
	consumerMapLock = make(chan bool, 1)
)

func createTopic(topicName string) error {
	broker := sarama.NewBroker("localhost:9092")
	err := broker.Open(nil)
	if err != nil {
		panic(err)
	}

	request := sarama.MetadataRequest{Topics: []string{topicName}}
	_, err = broker.GetMetadata(&request)
	if err != nil {
		_ = broker.Close()
		return err
	}

	// log.Printf("create Topic is %v", response.Topics)
	// log.Println("There are", len(response.Topics), "topics active in the cluster.")

	if err = broker.Close(); err != nil {
		return err
	}
	return nil
}

func getProducer(name string) (*sarama.AsyncProducer, error) {
	producerMapLock <- true
	defer func() {
		<-producerMapLock
	}()

	producer, ok := producerMap[name]
	if !ok {
		producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, nil)
		if err != nil {
			return nil, err
		}
		producerMap[name] = &producer
		return &producer, nil
	} else {
		return producer, nil
	}
}

func getConsumer(name string) *sarama.Consumer {
	producerMapLock <- true
	defer func() {
		<-producerMapLock
	}()

	consumer, ok := consumerMap[name]
	if !ok {
		consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, nil)
		if err != nil {
			panic(err)
		}
		consumerMap[name] = &consumer
		return &consumer
	} else {
		return consumer
	}
}

func sendMessage(producer *sarama.AsyncProducer, topic string, key string, value string) {
	// log.Printf("send msg to %s", topic)
	(*producer).Input() <- &sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(key), Value: sarama.StringEncoder(value)}
}

func doTopicCreate(level1 int, level2 int) {

	topic := fmt.Sprintf("nq_%v_%v", level1, level2)
	createTopic(topic)

	for x := 0; x < level1; x++ {
		for y := 0; y < level2; y++ {
			topic := fmt.Sprintf("nq_%v_%v_%v_%v", level1, level2, x, y)
			createTopic(topic)
		}
	}
}

func handleMessage(consumer *sarama.Consumer, topic string, handler func(msg *sarama.ConsumerMessage)) {
	go func() {
		partitionConsumer, err := (*consumer).ConsumePartition(topic, 0, sarama.OffsetOldest)
		if err != nil {
			log.Printf("err is %v", err)
			return
		}

		for {
			select {
			case msg := <-partitionConsumer.Messages():
				handler(msg)
			}
		}
	}()
}

func addNode(maxLevel1, maxLevel2, level1, level2 int) {
	nodeName := getNodeTopicName("node", maxLevel1, maxLevel2, level1, level2)
	topicName := getNodeTopicName("request", maxLevel1, maxLevel2, level1, level2)
	subTopicName := getSubTopicName("request", maxLevel1, maxLevel2)
	subResponseTopicName := getSubTopicName("response", maxLevel1, maxLevel2)

	nodeConsumer := getConsumer(nodeName)
	handler := func(msg *sarama.ConsumerMessage) {
		log.Printf("Node %s get message,%s=%s, offset=%v", nodeName, msg.Key, msg.Value, msg.Offset)
		nodeProducer, _ := getProducer(nodeName)
		sendMessage(nodeProducer, subResponseTopicName, fmt.Sprintf("NODE=%s, done!!!", nodeName), "value")
	}
	handleMessage(nodeConsumer, topicName, handler)
	handleMessage(nodeConsumer, subTopicName, handler)

}

func addAllNode(maxLevel1, maxLevel2 int) {
	for x := 0; x < maxLevel1; x++ {
		for y := 0; y < maxLevel2; y++ {
			addNode(maxLevel1, maxLevel2, x, y)
		}
	}
}

func getNodeTopicNameByID(topicPrefix string, id int64, maxLevel1, maxLevel2 int) string {
	level1 := id % int64(maxLevel1)
	level2 := id % int64(maxLevel2)
	return getNodeTopicName(topicPrefix, maxLevel1, maxLevel2, int(level1), int(level2))
}

func getNodeTopicName(topicPrefix string, maxLevel1, maxLevel2, level1, level2 int) string {
	return fmt.Sprintf("%s_%v_%v_%v_%v", topicPrefix, maxLevel1, maxLevel2, level1, level2)
}

func getSubTopicName(topicPrefix string, maxLevel1, maxLevel2 int) string {
	return fmt.Sprintf("%s_%v_%v", topicPrefix, maxLevel1, maxLevel2)
}
