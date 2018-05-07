package yxsdb

import (
	"database/sql"
	"errors"
	"fmt"
	"path"
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

func (node *DataNode) InitChildrenNodeTable(database []int, tableName int, columns []int, numberColumnMap map[int]string, keyColumnMap map[int]string, indexColumns []int) error {
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
