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
	db, err := yxsdb.CreateDatabase("target/0/", []int{0, 1, 2, 3}, []int{4, 5, 6, 7})
	if err != nil {
		t.Error(err)
	}

	dblist := yxsdb.GetDatabaseList(db)
	for i, db := range dblist {
		t.Logf("%v, name=%s, path=%s", i, db.Name, db.FilePath)
	}

}
