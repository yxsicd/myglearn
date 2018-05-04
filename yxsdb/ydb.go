package main

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

func main() {
	log.Printf("begin")

	// doTopicCreate(3, 5)
	// createTopic("noderequest")
	// doProducer()
	// doConsumer()

	log.Printf("end")
}
