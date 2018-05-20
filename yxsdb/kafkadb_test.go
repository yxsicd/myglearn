package yxsdb

import (
	"encoding/json"
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
		var request NodeRequest
		json.Unmarshal(msg.Value, &request)
		if strings.Compare(request.RequestType, "execute") == 0 {
			log.Printf("DO execute")
			tableName := request.TableName
			ret := node.ExecuteNodeTable(tableName, request.NodeSQL)
			if ret.err != nil {
				log.Printf("ret is %v", ret)
				// return
			}
			log.Printf("query ret is %v", ret)
			node.SendNodeResponse(&NodeResponse{RequestType: request.RequestType,
				ExecuteTaskResult: *ret})
		}

		if strings.Compare(request.RequestType, "query") == 0 {
			log.Printf("DO query")
			tableName := request.TableName
			nodeSQL := request.NodeSQL
			mergeSQL := request.MergeSQL
			if mergeSQL == "" {
				mergeSQL = nodeSQL
			}
			ret := node.QueryNodeTable(tableName, nodeSQL, mergeSQL)
			if ret.err != nil {
				log.Printf("ret is %v", ret)
				// return
			}
			ret.CacheTable.RowsShowCount = 3
			// log.Printf("query ret is %v", ret)
			node.SendNodeResponse(&NodeResponse{RequestType: request.RequestType,
				TableName:       request.TableName,
				QueryTaskResult: QTaskResult{CacheTable: *(ret.CacheTable.GetJSONTable()), err: ret.err}})
		}

	}

	kgroup1.HandleMessage(consumer, "request", 0, requestHandler)

}

func addMasterNodeKGroup(node *DataNode) {
	kgroup1 := InitKafkaGroup("localhost:9092")
	node.KafkaGroup = kgroup1
	consumer := kgroup1.GetConsumer("master")

	responseHandler := func(msg *sarama.ConsumerMessage, group *KafkaGroup) {
		// log.Printf("Node=%v get message, key=%s   value=%s   offset=%v, topic=%s", node.ID, msg.Key, msg.Value, msg.Offset, msg.Topic)
		var response NodeResponse
		json.Unmarshal(msg.Value, &response)
		log.Printf("unmarshal %s is %v", msg.Value, response.QueryTaskResult)

	}

	kgroup1.HandleMessage(consumer, "response", 0, responseHandler)

}

type NodeRequest struct {
	RequestType string
	TableName   int
	NodeSQL     string
	MergeSQL    string
}

type QTaskResult struct {
	CacheTable CacheTable
	err        error
}

type NodeResponse struct {
	RequestType       string
	TableName         int
	QueryTaskResult   QTaskResult
	ExecuteTaskResult ExecuteTaskResult
}

func (node *DataNode) SendNodeRequest(request *NodeRequest) error {
	retString, err := json.Marshal(request)
	if err != nil {
		return err
	}
	producer, err := node.KafkaGroup.GetProducer("request")
	if err != nil {
		return err
	}
	node.KafkaGroup.SendMessage(producer, "request", request.RequestType, string(retString))
	return nil
}

func (node *DataNode) SendNodeResponse(response *NodeResponse) error {
	retString, err := json.Marshal(response)
	if err != nil {
		return err
	}
	producer, err := node.KafkaGroup.GetProducer("response")
	if err != nil {
		return err
	}
	node.KafkaGroup.SendMessage(producer, "response", response.RequestType, string(retString))
	return nil
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

	// producer, _ := masterNode.KafkaGroup.GetProducer("request")
	// masterNode.KafkaGroup.SendMessage(producer, "request", "execute", "create table if not exists _0._4030(_0,_1,_2);")
	// masterNode.KafkaGroup.SendMessage(producer, "request", "execute", "insert into _0._4030 select 1,2,3;")
	// masterNode.KafkaGroup.SendMessage(producer, "request", "query", "select * from _0._4030;")

	masterNode.SendNodeRequest(&NodeRequest{RequestType: "execute", TableName: 4030, NodeSQL: "create table if not exists _0._4030(_0,_1,_2);"})
	masterNode.SendNodeRequest(&NodeRequest{RequestType: "execute", TableName: 4030, NodeSQL: "insert into _0._4030 select 1,2,3;"})
	masterNode.SendNodeRequest(&NodeRequest{RequestType: "query", TableName: 4030,
		NodeSQL:  "select count(_0) as _0 from _0._4030;",
		MergeSQL: "select sum(_0) from _0._4030;",
	})

	time.Sleep(10 * time.Second)
	log.Printf("end")
}
