package yxsdb

import (
	"testing"
	"yxsicd/yxsdb"
)

func TestGetName(t *testing.T) {

	databaseName := yxsdb.GetDatabaseName(0)
	tableName := yxsdb.GetTableName(4030)
	databaseTableName := yxsdb.GetDatabaseTableName(0, 4030)
	memoryDatabase := yxsdb.GetAttachDatabaseSql("target/data", 0, true)
	diskDatabase := yxsdb.GetAttachDatabaseSql("target/data", 0, false)

	t.Logf("database=0, tableName=4030, test result:\n database=%s\n tableName=%s\n databaseTableName=%s\n memoryDatabase=%s\n diskDatabase=%s\n",
		databaseName, tableName, databaseTableName, memoryDatabase, diskDatabase)
}

func TestCreateDatabase(t *testing.T) {
	_, err := yxsdb.CreateDatabase("target/0/", []int{0, 1, 2, 3}, []int{4, 5, 6, 7})
	if err != nil {
		t.Error(err)
	}
}

func TestGetDatabaseList(t *testing.T) {
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

}
