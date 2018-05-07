package yxsdb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"path"
	"testing"
)

func InitNode(t *testing.T) *DataNode {
	var cnodes []*DataNode
	node := DataNode{
		ID:             1,
		BaseDir:        "target/data",
		DiskDatabase:   []int{0, 1, 2},
		MemoryDatabase: []int{3, 4, 5, 6, 7, 8, 9},
		NodeLock:       make(chan bool, 1),
		ConnectionPool: make(map[string]*sql.DB),
	}

	for i := 0; i < 8; i++ {
		cnode := DataNode{
			ID:             i,
			BaseDir:        path.Join(node.BaseDir, fmt.Sprintf("%v", node.ID), "nodes"),
			DiskDatabase:   []int{0, 1, 2, 3},
			MemoryDatabase: []int{4, 5, 6, 7, 8, 9},
			NodeLock:       make(chan bool, 1),
			ConnectionPool: make(map[string]*sql.DB),
			ParentNode:     &node,
		}
		cnodes = append(cnodes, &cnode)
	}
	node.ChildrenNode = cnodes
	return &node
}

func TestCreateTable(t *testing.T) {
	node := InitNode(t)
	ret, err := json.Marshal(*node)
	t.Logf("node is %s", ret)
	columns := []int{0, 1, 2, 3, 4, 5}
	err = node.InitCNodeTable([]int{0}, 2010, columns,
		map[int]string{0: "", 1: ""}, map[int]string{},
		columns)
	if err != nil {
		t.Error(err)
	}
	var rows [][]interface{}
	for r := 0; r < 100; r++ {
		var row []interface{}
		for c := 0; c < 6; c++ {
			if c == 0 {
				row = append(row, r)
			} else {
				row = append(row, fmt.Sprintf("v-%v-%v", r, c))
			}
		}
		rows = append(rows, row)
	}
	db, err := node.GetCNodeDB(0, 2010)
	if err != nil {
		t.Error(err)
	}
	err = InsertRows(db, 0, 2010, columns, rows)
	if err != nil {
		t.Error(err)
	}
	retTable, err := QueryTable(db, "select * from _0._2010;")
	retTable.RowsShowCount = 3
	if err != nil {
		t.Error(err)
	}
	t.Logf("%s", retTable)

}
