package yxsdb

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"path"
)

var (
	globalID = 0
)

//DataNode Sqlite Data dir
type DataNode struct {
	ID              int
	BaseDir         string
	ConnectionPool  map[string]*sql.DB
	NodeLock        chan bool
	DiskDatabase    []int
	MemoryDatabase  []int
	ChildrenNode    []*DataNode
	ParentNode      *DataNode
	ChildrenNodeMap map[int]*DataNode
}

func (node *DataNode) GetTablePath(tableName int) string {
	tableBaseDir := path.Join(node.BaseDir, fmt.Sprintf("%v", node.ID), fmt.Sprintf("%v", tableName))
	return tableBaseDir
}

func (node *DataNode) GetDB(tableName int) (*sql.DB, error) {
	node.NodeLock <- true
	defer func() {
		<-node.NodeLock
	}()
	tableBaseDir := node.GetTablePath(tableName)
	db, ok := node.ConnectionPool[tableBaseDir]
	if ok {
		return db, nil
	}
	newdb, err := CreateDatabase(tableBaseDir, node.DiskDatabase, node.MemoryDatabase)
	if err != nil {
		return newdb, err
	}
	_, err = InitAttachDatabase(tableBaseDir, newdb, node.DiskDatabase, node.MemoryDatabase)
	if err != nil {
		return newdb, err
	}
	node.ConnectionPool[tableBaseDir] = newdb
	return newdb, err
}

func (node *DataNode) GetCNodeDB(nodeID, tableName int) (*sql.DB, error) {
	node.NodeLock <- true
	defer func() {
		<-node.NodeLock
	}()
	cnode, ok := node.ChildrenNodeMap[nodeID]
	if !ok {
		return nil, errors.New("nodeID not found")
	}
	return cnode.GetDB(tableName)
}

func (node *DataNode) InitNodeTable(database []int, tableName int, columns []int, numberColumnMap map[int]string, keyColumnMap map[int]string, indexColumns []int) error {
	db, err := node.GetDB(tableName)
	if err != nil {
		return err
	}
	for _, dbid := range database {
		err = InitTable(db, dbid, tableName, columns, numberColumnMap, keyColumnMap, indexColumns)
		if err != nil {
			return err
		}
	}
	return nil
}

func (node *DataNode) InitCNodeTable(database []int, tableName int, columns []int, numberColumnMap map[int]string, keyColumnMap map[int]string, indexColumns []int) error {
	node.NodeLock <- true
	defer func() {
		<-node.NodeLock
	}()
	node.ChildrenNodeMap = make(map[int]*DataNode)
	for _, cnode := range node.ChildrenNode {
		err := cnode.InitNodeTable(database, tableName, columns, numberColumnMap, keyColumnMap, indexColumns)
		if err != nil {
			return err
		}
		node.ChildrenNodeMap[cnode.ID] = cnode
	}
	return nil
}

func (node *DataNode) QueryTable(tableName int, querySQL string) (*CacheTable, error) {
	db, err := node.GetDB(tableName)
	if err != nil {
		return nil, err
	}
	retTable, err := QueryTable(db, querySQL)
	return retTable, err
}

func (node *DataNode) QueryNodeTable(tableName int, querySQL string, mergeSQL string) (*CacheTable, error) {

	globalID++
	queryID := globalID
	queryDone := make(chan bool, len(node.ChildrenNode))
	queryResult := make(chan *CacheTable, len(node.ChildrenNode))
	allDone := make(chan bool, 1)
	allDone <- true
	for _, cnode := range node.ChildrenNode {
		go func(queryNode *DataNode, tableName int, querySQL string) {
			queryDone <- true
			defer func() {
				<-queryDone
			}()
			retTable, err := queryNode.QueryTable(tableName, querySQL)
			if err != nil {
				return
			}
			retTable.RowsShowCount = 3
			log.Printf("%s", retTable)
			queryResult <- retTable
		}(cnode, tableName, querySQL)
	}

	go func() {
		for _ = range node.ChildrenNode {
			queryDone <- true
		}
		<-allDone
	}()

	resultCount := 0
	var allRet CacheTable
	// node.InitNodeTable([]int{0},queryID)
	select {
	case nodeResultTable := <-queryResult:
		resultCount++
		tableName := int(queryID)
		columns := []int{}
		if resultCount == 0 {
			columns = nodeResultTable.Columns
			node.InitNodeTable([]int{0}, tableName, columns, map[int]string{}, map[int]string{}, []int{})
		}
		db, err := node.GetDB(tableName)
		if err != nil {
			return nil, err
		}
		InsertRows(db, 0, tableName, columns, nodeResultTable.Rows)
		if resultCount == len(node.ChildrenNode) {
			allQueryResult, err := QueryTable(db, mergeSQL)
			if err != nil {
				return nil, err
			}
			fmt.Printf("allQueryResult %s", allQueryResult)
			allRet = *allQueryResult
			break
		}
	}
	return &allRet, nil

}
