package yxsdb

import (
	"fmt"
	"testing"
)

func TestGetName(t *testing.T) {

	databaseName := GetDatabaseName(0)
	tableName := GetTableName(4030)
	databaseTableName := GetDatabaseTableName(0, 4030)
	memoryDatabase := GetAttachDatabaseSQL("target/data/0/4030", 0, true)
	diskDatabase := GetAttachDatabaseSQL("target/data/0/4030", 0, false)

	t.Logf("database=0, tableName=4030, test result:\n database=%s\n tableName=%s\n databaseTableName=%s\n memoryDatabase=%s\n diskDatabase=%s\n",
		databaseName, tableName, databaseTableName, memoryDatabase, diskDatabase)
}

func TestCreateDatabase(t *testing.T) {
	_, err := CreateDatabase("target/data/0/4030", []int{0, 1, 2, 3}, []int{4, 5, 6, 7})
	if err != nil {
		t.Error(err)
	}
}
func TestInitDatabase(t *testing.T) {
	basePath := "target/data/0/4030"
	db, err := CreateDatabase(basePath, []int{0, 1, 2, 3}, []int{4, 5, 6, 7})
	if err != nil {
		t.Error(err)
	}
	DetachDatabase(db, 7)
	AttachDatabase(basePath, db, 7, false)
	AttachDatabase(basePath, db, 8, true)
	DetachDatabase(db, 8)
	AttachDatabase(basePath, db, 8, false)

	dblist := GetDatabaseList(db)
	for i, db := range dblist {
		t.Logf("%v, name=%s, path=%s", i, db.Name, db.FilePath)
	}

	dbmap := GetDatabaseMap(db)
	for k, v := range dbmap {
		t.Logf("name=%s, path=%s", k, v)
	}

	columns := []int{0, 1, 2, 3, 4, 5}

	t.Logf("CreateTable is %s", GetCreateTableSQL(0, 4030, columns, map[int]string{1: "", 2: ""}, map[int]string{0: ""}))
	t.Logf("CreateTableIndex is %s", GetCreateTableIndexSQL(0, 4030, 0))
	t.Logf("InitTableSql is %s", GetInitTableSQL(0, 4030, columns, map[int]string{0: "", 1: "", 2: ""}, map[int]string{0: ""}, columns))
	err = InitTable(db, 0, 4030, columns, map[int]string{0: "", 1: "", 2: ""}, map[int]string{0: ""}, columns)
	if err != nil {
		t.Error(err)
	}

	t.Logf("GetInsertSQL is %s", GetInsertSQL(0, 4030, 2, columns))
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
	err = ClearTable(db, 0, 4030)
	if err != nil {
		t.Error(err)
	}
	err = BatchInsertRows(db, 0, 4030, 5, columns, rows)
	if err != nil {
		t.Error(err)
	}

	err = ClearTable(db, 0, 4030)
	if err != nil {
		t.Error(err)
	}
	err = InsertRows(db, 0, 4030, columns, rows[32:])
	if err != nil {
		t.Error(err)
	}

	table, err := QueryTable(db, "select count(_0) as xsadf from _0._4030 order by _0 ")
	if err != nil {
		t.Error(err)
	}
	table.RowsShowCount = 10
	t.Log(table)
}
