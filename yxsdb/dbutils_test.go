package yxsdb

import (
	"testing"
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
