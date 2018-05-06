package yxsdb

import (
	"database/sql"
	"fmt"
	"testing"
)

func InitNode(t *testing.T) *DataNode {
	node := DataNode{
		ID:             1,
		BaseDir:        "target/data",
		DiskDatabase:   []int{0, 3, 4, 5},
		MemoryDatabase: []int{1, 2},
		NodeLock:       make(chan bool, 1),
		ConnectionPool: make(map[string]*sql.DB),
	}
	return &node
}

func TestCreateTable(t *testing.T) {
	node := InitNode(t)
	db, err := node.GetDB(2010)
	if err != nil {
		t.Error(err)
	}
	columns := []int{0, 1, 2, 3, 4, 5}
	err = InitTable(db, 0, 2010, columns,
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
