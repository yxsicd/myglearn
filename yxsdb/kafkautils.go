package yxsdb

import (
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

type KafkaGroup struct {
	Connection      string
	producerMap     map[string]*sarama.AsyncProducer
	producerMapLock chan bool
	consumerMap     map[string]*sarama.Consumer
	consumerMapLock chan bool
}

type KafkaNode struct {
	ID string
}

func InitKafkaGroup(connection string) *KafkaGroup {
	kgroup := KafkaGroup{Connection: connection,
		producerMap:     make(map[string]*sarama.AsyncProducer),
		producerMapLock: make(chan bool, 1),
		consumerMap:     make(map[string]*sarama.Consumer),
		consumerMapLock: make(chan bool, 1),
	}
	return &kgroup
}

func (group *KafkaGroup) CreateTopic(topicName string) error {
	broker := sarama.NewBroker(group.Connection)
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
	if err = broker.Close(); err != nil {
		return err
	}
	return nil
}

func (group *KafkaGroup) GetProducer(name string) (*sarama.AsyncProducer, error) {
	group.producerMapLock <- true
	defer func() {
		<-group.producerMapLock
	}()

	producer, ok := group.producerMap[name]
	if !ok {
		producer, err := sarama.NewAsyncProducer([]string{group.Connection}, nil)
		if err != nil {
			return nil, err
		}
		group.producerMap[name] = &producer
		return &producer, nil
	} else {
		return producer, nil
	}
}

func (group *KafkaGroup) GetConsumer(name string) *sarama.Consumer {
	group.producerMapLock <- true
	defer func() {
		<-group.producerMapLock
	}()

	consumer, ok := group.consumerMap[name]
	if !ok {
		consumer, err := sarama.NewConsumer([]string{group.Connection}, nil)
		if err != nil {
			panic(err)
		}
		group.consumerMap[name] = &consumer
		return &consumer
	} else {
		return consumer
	}
}

func (group *KafkaGroup) SendMessage(producer *sarama.AsyncProducer, topic string, key string, value string) {
	// log.Printf("send msg to %s", topic)
	(*producer).Input() <- &sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(key), Value: sarama.StringEncoder(value)}
}

func (group *KafkaGroup) BatchTopicCreate(maxLevel1 int, maxLevel2 int) {

	topic := group.GetSubTopicName("request", maxLevel1, maxLevel2)
	group.CreateTopic(topic)

	for x := 0; x < maxLevel1; x++ {
		for y := 0; y < maxLevel2; y++ {
			topicName := group.GetNodeTopicName("request", maxLevel1, maxLevel2, x, y)
			group.CreateTopic(topicName)
		}
	}
}

func (group *KafkaGroup) HandleMessage(consumer *sarama.Consumer, topic string, partition int32, beginOffset int64, handler func(msg *sarama.ConsumerMessage, group *KafkaGroup)) {
	go func() {
		partitionConsumer, err := (*consumer).ConsumePartition(topic, partition, beginOffset)
		if err != nil {
			log.Printf("err is %v", err)
			return
		}

		for {
			select {
			case msg := <-partitionConsumer.Messages():
				handler(msg, group)
			}
		}
	}()
}

func (group *KafkaGroup) AddNode(maxLevel1, maxLevel2, level1, level2 int) {
	nodeName := group.GetNodeTopicName("node", maxLevel1, maxLevel2, level1, level2)
	topicName := group.GetNodeTopicName("request", maxLevel1, maxLevel2, level1, level2)
	subTopicName := group.GetSubTopicName("request", maxLevel1, maxLevel2)
	subResponseTopicName := group.GetSubTopicName("response", maxLevel1, maxLevel2)

	nodeConsumer := group.GetConsumer(nodeName)
	handler := func(msg *sarama.ConsumerMessage, group *KafkaGroup) {
		log.Printf("Node %s get message,%s=%s, offset=%v, topic=%s", nodeName, msg.Key, msg.Value, msg.Offset, msg.Topic)
		nodeProducer, _ := group.GetProducer(nodeName)
		group.SendMessage(nodeProducer, subResponseTopicName, fmt.Sprintf("NODE=%s, done!!!", nodeName), "value")
	}
	group.HandleMessage(nodeConsumer, topicName, 0, 0, handler)
	group.HandleMessage(nodeConsumer, subTopicName, 0, 0, handler)

}

func (group *KafkaGroup) AddAllNode(maxLevel1, maxLevel2 int) {
	for x := 0; x < maxLevel1; x++ {
		for y := 0; y < maxLevel2; y++ {
			group.AddNode(maxLevel1, maxLevel2, x, y)
		}
	}
}

func (group *KafkaGroup) GetNodeTopicNameByID(topicPrefix string, id int64, maxLevel1, maxLevel2 int) string {
	level1 := id % int64(maxLevel1)
	level2 := id % int64(maxLevel2)
	return group.GetNodeTopicName(topicPrefix, maxLevel1, maxLevel2, int(level1), int(level2))
}

func (group *KafkaGroup) GetNodeTopicName(topicPrefix string, maxLevel1, maxLevel2, level1, level2 int) string {
	return fmt.Sprintf("%s_%v_%v_%v_%v", topicPrefix, maxLevel1, maxLevel2, level1, level2)
}

func (group *KafkaGroup) GetSubTopicName(topicPrefix string, maxLevel1, maxLevel2 int) string {
	return fmt.Sprintf("%s_%v_%v", topicPrefix, maxLevel1, maxLevel2)
}
