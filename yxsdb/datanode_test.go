package yxsdb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"path"
	"testing"
)

func InitNode() *DataNode {
	var cnodes []*DataNode
	node := DataNode{
		ID:             1,
		BaseDir:        "target/data",
		DiskDatabase:   []int{1},
		MemoryDatabase: []int{0},
		NodeLock:       make(chan bool, 1),
		ConnectionPool: make(map[string]*sql.DB),
	}

	for i := 0; i < 8; i++ {
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

func TestCreateTable(t *testing.T) {
	node := InitNode()
	ret, err := json.Marshal(*node)
	t.Logf("node is %s", ret)
	columns := []int{0, 1, 2, 3, 4, 5}
	err = node.InitCNodeTable([]int{0, 1, 2, 3, 4, 5, 6, 7}, 2010, columns,
		map[int]string{0: "", 1: ""}, map[int]string{},
		columns)
	if err != nil {
		t.Error(err)
	}
	var rows [][]interface{}
	for r := 0; r < 5000; r++ {
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

	for n := 0; n < 8; n++ {
		tableName := 2010
		db, err := node.GetCNodeDB(n, tableName)
		if err != nil {
			t.Error(err)
		}
		err = InsertRows(db, 0, tableName, columns, rows)
		if err != nil {
			t.Error(err)
		}
		retTable, err := QueryTable(db, fmt.Sprintf("select count(_0) from _%v._%v ;", 0, tableName))
		retTable.RowsShowCount = 3
		if err != nil {
			t.Error(err)
		}
		t.Logf("%s", retTable)
	}
}

var (
	testNode = InitNode()
)

func BenchmarkInsert(t *testing.B) {
	node := InitNode()
	ret, err := json.Marshal(*node)
	t.Logf("node is %s", ret)
	columns := []int{0, 1, 2, 3, 4, 5}
	err = node.InitCNodeTable([]int{0, 1, 2, 3, 4, 5, 6, 7}, 2010, columns,
		map[int]string{0: "", 1: ""}, map[int]string{},
		columns)
	if err != nil {
		t.Error(err)
	}

	var rows [][]interface{}
	for r := 0; r < 10; r++ {
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

	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		for n := 0; n < 1; n++ {
			tableName := 2010
			db, err := node.GetCNodeDB(n, tableName)
			if err != nil {
				t.Error(err)
			}
			err = InsertRows(db, 0, tableName, columns, rows)
			if err != nil {
				t.Error(err)
			}
			retTable, err := QueryTable(db, fmt.Sprintf("select count(_0) from _%v._%v ;", 0, tableName))
			retTable.RowsShowCount = 3
			if err != nil {
				t.Error(err)
			}
			t.Logf("%s", retTable)
		}
	}
}

func BenchmarkNodeQuery(t *testing.B) {
	node := InitNode()
	tableName := 2010
	ret, err := json.Marshal(*node)
	t.Logf("node is %s", ret)
	columns := []int{0, 1, 2, 3, 4, 5}
	err = node.InitCNodeTable([]int{0, 1}, tableName, columns,
		map[int]string{0: "", 1: ""}, map[int]string{},
		columns)
	if err != nil {
		t.Error(err)
	}

	var rows [][]interface{}
	for r := 0; r < 1000; r++ {
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

	for n := 0; n < 8; n++ {
		db, err := node.GetCNodeDB(n, tableName)
		if err != nil {
			t.Error(err)
		}
		err = InsertRows(db, 0, tableName, columns, rows)
		if err != nil {
			t.Error(err)
		}
	}

	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		querySQL := fmt.Sprintf("select count(_0) as _0 from _%v._%v ;", 0, tableName)
		mergeSQL := fmt.Sprintf("select sum(_0) from _%v._%v ;", 0, tableName)
		queryResult := node.QueryNodeTable(tableName, querySQL, mergeSQL)
		if queryResult.err != nil {
			t.Error(queryResult.err)
		}
		queryResult.CacheTable.RowsShowCount = 3
		t.Logf("%s", queryResult.CacheTable)
	}

	// t.ResetTimer()
	// for i := 0; i < t.N; i++ {
	// 	db, err := node.GetCNodeDB(0, tableName)
	// 	if err != nil {
	// 		t.Error(err)
	// 	}
	// 	retTable, err := QueryTable(db, fmt.Sprintf("select * from _%v._%v limit 10;", 0, tableName))
	// 	retTable.RowsShowCount = 3
	// 	if err != nil {
	// 		t.Error(err)
	// 	}
	// 	t.Logf("%s", retTable)
	// }

}

func TestNodeQuery(t *testing.T) {
	node := InitNode()
	tableName := 2010
	ret, err := json.Marshal(*node)
	t.Logf("node is %s", ret)
	columns := []int{0, 1, 2, 3, 4, 5}
	err = node.InitCNodeTable([]int{0, 1}, tableName, columns,
		map[int]string{0: "", 1: ""}, map[int]string{},
		columns)
	if err != nil {
		t.Error(err)
	}

	var rows [][]interface{}
	for r := 0; r < 1000; r++ {
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

	for n := 0; n < 8; n++ {
		db, err := node.GetCNodeDB(n, tableName)
		if err != nil {
			t.Error(err)
		}
		err = InsertRows(db, 0, tableName, columns, rows)
		if err != nil {
			t.Error(err)
		}
	}

	querySQL := fmt.Sprintf("select count(_0) as _0 from _%v._%v ;", 0, tableName)
	mergeSQL := fmt.Sprintf("select sum(_0) from _%v._%v ;", 0, tableName)

	log.Printf("querySQL=%s\nmergeSQL=%s", querySQL, mergeSQL)

	for n := 0; n < 8; n++ {
		retTable, err := node.ChildrenNodeMap[n].QueryTable(tableName, querySQL)
		if err != nil {
			t.Error(err)
		}
		retTable.RowsShowCount = 3
		t.Logf("%s", retTable)
	}

	queryResult := node.QueryNodeTable(tableName, querySQL, mergeSQL)
	if queryResult.err != nil {
		t.Error(queryResult.err)
	}
	queryResult.CacheTable.RowsShowCount = 3
	t.Logf("%s", queryResult.CacheTable)
}

func ExampleSalutations() {
	fmt.Println("hello, and")
	fmt.Println("goodbye")
	// Output:
	// hello, and
	// goodbye
}
