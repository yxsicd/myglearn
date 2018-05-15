package yxsdb

import (
	"encoding/json"
	"fmt"
	"log"
	"testing"
)

func TestCreateTable(t *testing.T) {
	node := InitNode(1, "/dev/shm/target/data", 8)
	columns := []int{0, 1, 2, 3, 4, 5}
	err := node.InitCNodeTable([]int{0, 1}, 2010, columns,
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
	testNode = InitNode(1, "/dev/shm/target/data", 8)
)

func BenchmarkInsert(t *testing.B) {
	node := InitNode(1, "/dev/shm/target/data", 8)
	columns := []int{0, 1, 2, 3, 4, 5}
	err := node.InitCNodeTable([]int{0, 1}, 2010, columns,
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
	node := InitNode(1, "/dev/shm/target/data", 8)
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
	// 	querySQL := fmt.Sprintf("select * from _%v._%v where _0 like '%%34%%' order by _0 desc limit 20;", 0, tableName)
	// 	mergeSQL := fmt.Sprintf("select * from _%v._%v where _0 like '%%34%%' order by _0 desc limit 20;", 0, tableName)
	// 	queryResult := node.QueryNodeTable(tableName, querySQL, mergeSQL)
	// 	if queryResult.err != nil {
	// 		t.Error(queryResult.err)
	// 	}
	// 	queryResult.CacheTable.RowsShowCount = 3
	// 	t.Logf("%s", queryResult.CacheTable)
	// }

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
	node := InitNode(1, "/dev/shm/target/data", 8)
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
