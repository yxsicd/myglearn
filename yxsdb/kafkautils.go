package yxsdb

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

var (
	producerMap     = make(map[string]*sarama.AsyncProducer)
	producerMapLock = make(chan bool, 1)

	consumerMap     = make(map[string]*sarama.Consumer)
	consumerMapLock = make(chan bool, 1)
)

func createTopic(topicName string) {
	broker := sarama.NewBroker("localhost:9092")
	err := broker.Open(nil)
	if err != nil {
		panic(err)
	}

	request := sarama.MetadataRequest{Topics: []string{topicName}}
	response, err := broker.GetMetadata(&request)
	if err != nil {
		_ = broker.Close()
		panic(err)
	}

	log.Println("There are", len(response.Topics), "topics active in the cluster.")

	if err = broker.Close(); err != nil {
		panic(err)
	}
}

func getProducer(name string) *sarama.AsyncProducer {
	producerMapLock <- true
	defer func() {
		<-producerMapLock
	}()

	producer, ok := producerMap[name]
	if !ok {
		producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, nil)
		if err != nil {
			panic(err)
		}
		producerMap[name] = &producer
		return &producer
	} else {
		return producer
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
	(*producer).Input() <- &sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(key), Value: sarama.StringEncoder(value)}
}

func doProducer() {
	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var enqueued, errors int
ProducerLoop:
	for i := 0; i < 1; i++ {
		select {
		case producer.Input() <- &sarama.ProducerMessage{Topic: "noderequest", Key: nil, Value: sarama.StringEncoder("testing 123")}:
			enqueued++
		case err := <-producer.Errors():
			log.Println("Failed to produce message", err)
			errors++
		case <-signals:
			break ProducerLoop
		}
	}

	log.Printf("Enqueued: %d; errors: %d\n", enqueued, errors)
}

func doConsumer() {
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition("noderequest", 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumed := 0
ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			log.Printf("Consumed message offset %d, key=value is %s=%s", msg.Offset, msg.Key, msg.Value)
			consumed++
		case <-signals:
			break ConsumerLoop
		}
	}

	log.Printf("Consumed: %d\n", consumed)
}

func doTopicCreate(level1 int, level2 int) {

	for x := 0; x < level1; x++ {
		for y := 0; y < level2; y++ {
			topic := fmt.Sprintf("nq-%v-%v", x, y)
			createTopic(topic)
		}
	}
}

func handleMessage(consumer *sarama.Consumer, topic string, handler func(msg *sarama.ConsumerMessage)) {

	go func() {
		partitionConsumer, err := (*consumer).ConsumePartition(topic, 0, sarama.OffsetOldest)
		if err != nil {
			panic(err)
		}

		for {
			select {
			case msg := <-partitionConsumer.Messages():
				// log.Printf("Consumed message offset=%d, %s=%s", msg.Offset, msg.Key, msg.Value)
				handler(msg)
			}
		}
	}()
}

func addNode(level1, level2 int) {
	nodeName := fmt.Sprintf("node-%v-%v", level1, level2)
	topicName := fmt.Sprintf("nq-%v-%v", level1, level2)

	nodeConsumer := getConsumer(nodeName)
	handler := func(msg *sarama.ConsumerMessage) {
		log.Printf("Node %s get message,%s=%s, offset=%v", nodeName, msg.Key, msg.Value, msg.Offset)
	}
	handleMessage(nodeConsumer, topicName, handler)
}

func addAllNode(maxLevel1, maxLevel2 int) {
	for x := 0; x < maxLevel1; x++ {
		for y := 0; y < maxLevel2; y++ {
			addNode(x, y)
		}
	}
}

func getTopicName(id int64, maxLevel1, maxLevel2 int) string {
	level1 := id % int64(maxLevel1)
	level2 := id % int64(maxLevel2)
	return fmt.Sprintf("nq-%v-%v", level1, level2)
}
