package yxsdb

import (
	"encoding/json"
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
		var requestKey NodeRequestKey
		var requestValue NodeRequestValue
		json.Unmarshal(msg.Key, &requestKey)
		json.Unmarshal(msg.Value, &requestValue)

		if strings.Compare(requestKey.RequestType, "execute") == 0 {
			// log.Printf("DO execute")
			tableName := requestKey.TableName
			ret := node.ExecuteNodeTable(tableName, requestValue.NodeSQL)
			// log.Printf("query ret is %v", ret)
			rkey := NodeResponseKey{RequestType: requestKey.RequestType, RequestID: requestKey.RequestID, TableName: requestKey.TableName}
			rvalue := NodeResponseValue{ExecuteTaskResult: *ret}
			node.SendNodeResponse(&NodeResponse{Key: rkey, Value: rvalue})
		}

		if strings.Compare(requestKey.RequestType, "query") == 0 {
			// log.Printf("DO query")
			tableName := requestKey.TableName
			nodeSQL := requestValue.NodeSQL
			mergeSQL := requestValue.MergeSQL
			if mergeSQL == "" {
				mergeSQL = nodeSQL
			}
			ret := node.QueryNodeTable(tableName, nodeSQL, mergeSQL)
			if ret.CacheTable != nil {
				ret.CacheTable.RowsShowCount = 3
			}
			// log.Printf("query ret is %v", ret)
			rkey := NodeResponseKey{RequestType: requestKey.RequestType, RequestID: requestKey.RequestID, TableName: requestKey.TableName}
			rvalue := NodeResponseValue{QueryTaskResult: QTaskResult{CacheTable: *(ret.CacheTable.GetJSONTable()), err: ret.err}}
			node.SendNodeResponse(&NodeResponse{Key: rkey, Value: rvalue})
		}

		node.ExecuteTable(0, fmt.Sprintf(`insert into _1._0 select 0,%v;
			update _0._0 set _1=(select _1 from _1._0 where _1._0._0=_0._0._0 );
			insert into _0._0 select * from _1._0 where _0 not in (select _0 from _0._0);
			delete from _1._0;`, msg.Offset))
	}
	node.ExecuteTable(0, fmt.Sprintf(`create table if not exists _0._0(_0 int,_1 int); create table if not exists _1._0(_0 int,_1 int);`))
	ret, _ := node.QueryTable(0, "select _0,_1 from _0._0;")
	beginOffset := int64(0)
	if len(ret.Rows) > 0 {
		beginOffset = (*(ret.Rows[0][1].(*interface{}))).(int64)
	}
	kgroup1.HandleMessage(consumer, "request", 0, beginOffset, requestHandler)

}

func addMasterNodeKGroup(node *DataNode) {
	kgroup1 := InitKafkaGroup("localhost:9092")
	node.KafkaGroup = kgroup1
	consumer := kgroup1.GetConsumer("master")

	responseHandler := func(msg *sarama.ConsumerMessage, group *KafkaGroup) {
		// log.Printf("Node=%v get message, key=%s   value=%s   offset=%v, topic=%s", node.ID, msg.Key, msg.Value, msg.Offset, msg.Topic)
		var response NodeResponseValue
		json.Unmarshal(msg.Value, &response)
		log.Printf("msg=%s", msg)

	}

	kgroup1.HandleMessage(consumer, "response", 0, 0, responseHandler)

}

type NodeRequestKey struct {
	RequestType string
	RequestID   string
	TableName   int
}

func (requestKey *NodeRequestKey) GetNodeResponseKey() *NodeResponseKey {
	return &NodeResponseKey{RequestType: requestKey.RequestType, RequestID: requestKey.RequestID, TableName: requestKey.TableName}
}

type NodeRequestValue struct {
	NodeSQL  string
	MergeSQL string
}

type NodeRequest struct {
	Key   NodeRequestKey
	Value NodeRequestValue
}

type NodeResponseKey struct {
	RequestType string
	RequestID   string
	TableName   int
}

type NodeResponse struct {
	Key   NodeResponseKey
	Value NodeResponseValue
}

type QTaskResult struct {
	CacheTable CacheTable
	err        error
}

type NodeResponseValue struct {
	QueryTaskResult   QTaskResult
	ExecuteTaskResult ExecuteTaskResult
}

func (node *DataNode) SendNodeRequest(request *NodeRequest) error {
	key, err := json.Marshal(request.Key)
	if err != nil {
		return err
	}
	value, err := json.Marshal(request.Value)
	if err != nil {
		return err
	}
	producer, err := node.KafkaGroup.GetProducer("request")
	if err != nil {
		return err
	}
	node.KafkaGroup.SendMessage(producer, "request", string(key), string(value))
	return nil
}

func (node *DataNode) SendNodeResponse(response *NodeResponse) error {
	key, err := json.Marshal(response.Key)
	if err != nil {
		return err
	}
	value, err := json.Marshal(response.Value)
	if err != nil {
		return err
	}
	producer, err := node.KafkaGroup.GetProducer("response")
	if err != nil {
		return err
	}
	node.KafkaGroup.SendMessage(producer, "response", string(key), string(value))
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

	createRequest := &NodeRequest{Key: NodeRequestKey{RequestType: "execute", RequestID: "1", TableName: 4030}, Value: NodeRequestValue{NodeSQL: "create table if not exists _0._4030(_0,_1,_2);"}}
	masterNode.SendNodeRequest(createRequest)
	for i := 0; i < 5; i++ {
		insertRequest := &NodeRequest{Key: NodeRequestKey{RequestType: "execute", RequestID: "2", TableName: 4030}, Value: NodeRequestValue{NodeSQL: "insert into _0._4030 select 1,2,3;"}}
		masterNode.SendNodeRequest(insertRequest)
	}
	queryRequest := &NodeRequest{Key: NodeRequestKey{RequestType: "query", RequestID: "3", TableName: 4030}, Value: NodeRequestValue{NodeSQL: "select count(_0) as _0 from _0._4030;",
		MergeSQL: "select sum(_0) from _0._4030;"}}

	masterNode.SendNodeRequest(queryRequest)

	time.Sleep(10 * time.Second)
	log.Printf("end")
}
