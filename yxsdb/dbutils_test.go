package yxsdb

import (
	"fmt"
	"testing"
	"yxsicd/yxsdb"
)

func TestGetName(t *testing.T) {

	databaseName := yxsdb.GetDatabaseName(0)
	tableName := yxsdb.GetTableName(4030)
	databaseTableName := yxsdb.GetDatabaseTableName(0, 4030)
	memoryDatabase := yxsdb.GetAttachDatabaseSQL("target/data", 0, true)
	diskDatabase := yxsdb.GetAttachDatabaseSQL("target/data", 0, false)

	t.Logf("database=0, tableName=4030, test result:\n database=%s\n tableName=%s\n databaseTableName=%s\n memoryDatabase=%s\n diskDatabase=%s\n",
		databaseName, tableName, databaseTableName, memoryDatabase, diskDatabase)
}

func TestCreateDatabase(t *testing.T) {
	_, err := yxsdb.CreateDatabase("target/data/0/", []int{0, 1, 2, 3}, []int{4, 5, 6, 7})
	if err != nil {
		t.Error(err)
	}
}
func TestInitDatabase(t *testing.T) {
	basePath := "target/data/0"
	db, err := yxsdb.CreateDatabase(basePath, []int{1, 2, 3}, []int{0, 4, 5, 6, 7})
	if err != nil {
		t.Error(err)
	}
	yxsdb.DetachDatabase(db, 7)
	yxsdb.AttachDatabase(basePath, db, 7, false)
	yxsdb.AttachDatabase(basePath, db, 8, true)
	yxsdb.DetachDatabase(db, 8)
	yxsdb.AttachDatabase(basePath, db, 8, false)

	dblist := yxsdb.GetDatabaseList(db)
	for i, db := range dblist {
		t.Logf("%v, name=%s, path=%s", i, db.Name, db.FilePath)
	}

	dbmap := yxsdb.GetDatabaseMap(db)
	for k, v := range dbmap {
		t.Logf("name=%s, path=%s", k, v)
	}

	columns := []int{0, 1, 2, 3, 4, 5}

	t.Logf("CreateTable is %s", yxsdb.GetCreateTableSQL(0, 4030, columns, map[int]string{1: "", 2: ""}, map[int]string{0: ""}))
	t.Logf("CreateTableIndex is %s", yxsdb.GetCreateTableIndexSQL(0, 4030, 0))
	t.Logf("InitTableSql is %s", yxsdb.GetInitTableSQL(0, 4030, columns, map[int]string{0: "", 1: "", 2: ""}, map[int]string{0: ""}, columns))
	err = yxsdb.InitTable(db, 0, 4030, columns, map[int]string{0: "", 1: "", 2: ""}, map[int]string{0: ""}, columns)
	if err != nil {
		t.Error(err)
	}

	t.Logf("GetInsertSQL is %s", yxsdb.GetInsertSQL(0, 4030, 2, columns))
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
	err = yxsdb.ClearTable(db, 0, 4030)
	if err != nil {
		t.Error(err)
	}
	err = yxsdb.BatchInsertRows(db, 0, 4030, 5, columns, rows)
	if err != nil {
		t.Error(err)
	}

	err = yxsdb.ClearTable(db, 0, 4030)
	if err != nil {
		t.Error(err)
	}
	err = yxsdb.InsertRows(db, 0, 4030, columns, rows[32:])
	if err != nil {
		t.Error(err)
	}

	table, err := yxsdb.QueryTable(db, "select * from _0._4030 order by _0 limit 20")
	if err != nil {
		t.Error(err)
	}
	table.RowsShowCount = 10
	t.Log(table)
}
