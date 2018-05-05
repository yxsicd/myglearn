package yxsdb

import (
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
	basePath := "target/0"
	db, err := yxsdb.CreateDatabase(basePath, []int{0, 1, 2, 3}, []int{4, 5, 6, 7})
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

	t.Logf("CreateTable is %s", yxsdb.GetCreateTableSQL(0, 4030, []int{0, 1, 2, 3, 4, 5}, map[int]string{1: "", 2: ""}, map[int]string{0: ""}))
	t.Logf("CreateTableIndex is %s", yxsdb.GetCreateTableIndexSQL(0, 4030, 0))
	t.Logf("InitTableSql is %s", yxsdb.GetInitTableSQL(0, 4030, []int{0, 1, 2, 3, 4, 5}, map[int]string{0: "", 1: "", 2: ""}, map[int]string{0: ""}, []int{0, 1, 2, 3, 4, 5}))
	err = yxsdb.InitTable(db, 0, 4030, []int{0, 1, 2, 3, 4, 5}, map[int]string{0: "", 1: "", 2: ""}, map[int]string{0: ""}, []int{0, 1, 2, 3, 4, 5})
	if err != nil {
		t.Error(err)
	}

	t.Logf("GetInsertSQL is %s", yxsdb.GetInsertSQL(0, 4030, 4, 3))
}
