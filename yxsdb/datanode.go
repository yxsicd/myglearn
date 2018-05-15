package yxsdb

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
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

func InitNode(NodeId int, nodePath string, cNodeCount int) *DataNode {
	var cnodes []*DataNode
	node := DataNode{
		ID:             NodeId,
		BaseDir:        nodePath,
		DiskDatabase:   []int{},
		MemoryDatabase: []int{0},
		NodeLock:       make(chan bool, 1),
		ConnectionPool: make(map[string]*sql.DB),
	}

	for i := 0; i < cNodeCount; i++ {
		cnode := DataNode{
			ID:             i,
			BaseDir:        path.Join(node.BaseDir, fmt.Sprintf("%v", node.ID), "nodes"),
			DiskDatabase:   []int{0},
			MemoryDatabase: []int{1},
			NodeLock:       make(chan bool, 1),
			ConnectionPool: make(map[string]*sql.DB),
			ParentNode:     &node,
		}
		cnodes = append(cnodes, &cnode)
	}
	node.ChildrenNode = cnodes
	return &node
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

type QueryTaskResult struct {
	CacheTable *CacheTable
	err        error
}

func (node *DataNode) DropTable(tableName int) error {
	node.NodeLock <- true
	defer func() {
		<-node.NodeLock
	}()
	tableBaseDir := node.GetTablePath(tableName)
	db, ok := node.ConnectionPool[tableBaseDir]
	if ok {
		db.Close()
	}
	delete(node.ConnectionPool, tableBaseDir)
	err := os.RemoveAll(tableBaseDir)
	return err
}

func (node *DataNode) QueryNodeTable(tableName int, querySQL string, mergeSQL string) *QueryTaskResult {

	globalID++
	queryID := globalID
	queryResult := make(chan *QueryTaskResult, len(node.ChildrenNode))
	for _, cnode := range node.ChildrenNode {
		go func(queryNode *DataNode, tableName int, querySQL string) {
			retTable, err := queryNode.QueryTable(tableName, querySQL)
			queryResult <- &QueryTaskResult{CacheTable: retTable, err: err}
		}(cnode, tableName, querySQL)
	}

	resultCount := 0
	// node.InitNodeTable([]int{0},queryID)
	for resultCount != len(node.ChildrenNode) {
		nodeTaskResult := <-queryResult
		if nodeTaskResult.err != nil {
			return nodeTaskResult
		}
		nodeResultTable := nodeTaskResult.CacheTable
		resultCount++
		queryTableName := int(queryID)
		if resultCount == 1 {
			node.InitNodeTable([]int{0}, queryTableName, nodeResultTable.Columns, map[int]string{}, map[int]string{}, []int{})
			defer func() {
				node.DropTable(queryTableName)
			}()
		}
		db, err := node.GetDB(queryTableName)
		if err != nil {
			return &QueryTaskResult{CacheTable: nil, err: err}
		}
		err = InsertRows(db, 0, queryTableName, nodeResultTable.Columns, nodeResultTable.Rows)
		if err != nil {
			return &QueryTaskResult{CacheTable: nil, err: err}
		}
	}
	replaceMergeSQL := strings.Replace(mergeSQL, fmt.Sprintf("_%v._%v", 0, tableName), fmt.Sprintf("_%v._%v", 0, queryID), -1)
	allQueryResult, err := node.QueryTable(queryID, replaceMergeSQL)
	if err != nil {
		return &QueryTaskResult{CacheTable: nil, err: err}
	}
	return &QueryTaskResult{CacheTable: allQueryResult, err: err}
}

type ExecuteTaskResult struct {
	Result *sql.Result
	err    error
}

func (node *DataNode) ExecuteTable(tableName int, executeSQL string) *ExecuteTaskResult {
	db, err := node.GetDB(tableName)
	if err != nil {
		return &ExecuteTaskResult{nil, err}
	}
	ret, err := db.Exec(executeSQL)
	if err != nil {
		return &ExecuteTaskResult{&ret, err}
	}
	return &ExecuteTaskResult{&ret, err}
}

func (node *DataNode) ExecuteNodeTable(tableName int, executeSQL string) *ExecuteTaskResult {
	executeResult := make(chan *ExecuteTaskResult, len(node.ChildrenNode))
	for _, cnode := range node.ChildrenNode {
		go func(queryNode *DataNode, tableName int, executeSQL string) {
			eret := queryNode.ExecuteTable(tableName, executeSQL)
			executeResult <- eret
		}(cnode, tableName, executeSQL)
	}

	resultCount := 0
	for resultCount != len(node.ChildrenNode) {
		nodeTaskResult := <-executeResult
		resultCount++
		if nodeTaskResult.err != nil {
			return nodeTaskResult
		}
	}
	return &ExecuteTaskResult{nil, nil}
}
