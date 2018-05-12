package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"
)

func tryCreateTopic(tname string) {
	broker := sarama.NewBroker("localhost:9092")
	err := broker.Open(nil)
	if err != nil {
		// log.Printf("%s", err)
	}

	req := sarama.ApiVersionsRequest{}
	res, err := broker.ApiVersions(&req)
	if err != nil {
		log.Printf("%s", err)
		return
	}
	log.Printf("%s", res)

	request := sarama.CreateTopicsRequest{
		TopicDetails: map[string]*sarama.TopicDetail{
			tname: &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 0},
		},
	}
	response, err := broker.CreateTopics(&request)
	if err != nil {
		log.Printf("%s", err)
		_ = broker.Close()
		return
	}

	fmt.Printf("There are %s %s", response.TopicErrors, "topics active in the cluster.")

	if err = broker.Close(); err != nil {
		// panic(err)
	}
}

func btopic() {
	broker := sarama.NewBroker("localhost:9092")
	err := broker.Open(nil)
	if err != nil {
		panic(err)
	}

	request := sarama.MetadataRequest{Topics: nil}
	response, err := broker.GetMetadata(&request)
	if err != nil {
		_ = broker.Close()
		panic(err)
	}

	response.AddTopic("tttt2", 0)

	for i, v := range response.Topics {

		fmt.Printf("version is %v, %v=%s, pcount is %v\n", response.Version, i, v.Name, len(v.Partitions))
	}

	if err = broker.Close(); err != nil {
		panic(err)
	}
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Llongfile)
	tryCreateTopic("xxxx")
	// btopic()
	// for i := 0; i < 3; i++ {
	// 	go sendMsg()
	// }
	// for i := 0; i < 30; i++ {
	// 	go testconsumer(i)
	// }

	select {}
}

func testconsumer(cid int) {
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition("tyxs", 0, sarama.OffsetNewest)
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
			if msg.Offset%100 == 0 {
				log.Printf("%v, Consumed message offset %d\n", cid, msg.Offset)
			}
			consumed++
		case <-signals:
			break ConsumerLoop
		}
	}

	log.Printf("Consumed: %d\n", consumed)
}

func sendMsg() {
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
	for {
		select {
		case producer.Input() <- &sarama.ProducerMessage{Topic: "tyxs", Key: sarama.StringEncoder("key 123"), Value: sarama.StringEncoder("testing 123")}:
			enqueued++
		case err := <-producer.Errors():
			log.Println("Failed to produce message", err)
			errors++
		case <-signals:
			break ProducerLoop
		}
		time.Sleep(time.Millisecond)
	}

	log.Printf("Enqueued: %d; errors: %d\n", enqueued, errors)
}
