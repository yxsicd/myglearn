package yxsdb

import (
	"database/sql"
	"fmt"
	"path"
)

//DataNode Sqlite Data dir
type DataNode struct {
	ID             int
	BaseDir        string
	ConnectionPool map[string]*sql.DB
	NodeLock       chan bool
	DiskDatabase   []int
	MemoryDatabase []int
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
