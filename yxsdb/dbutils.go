package yxsdb

import (
	"database/sql"
	"fmt"
	"os"
	"path"
	"strings"
)

func GetDatabaseTableName(database, tableName int) string {
	return fmt.Sprintf("%s.%s", GetDatabaseName(database), GetTableName(tableName))
}

func GetTableName(tableName int) string {
	return fmt.Sprintf("_%v", tableName)
}

func GetDatabaseName(database int) string {
	return fmt.Sprintf("_%v", database)
}

type Database struct {
	Name     string
	FilePath string
}

func GetDatabaseList(db *sql.DB) []Database {
	var databaseList []Database
	rows, err := db.Query("pragma database_list;")
	if err != nil {
		return databaseList
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id   string
			name string
			path string
		)
		err = rows.Scan(&id, &name, &path)
		if err != nil {
			return databaseList
		}
		database := Database{Name: name, FilePath: path}
	}
}

func GetAttachDatabaseSql(baseDir string, database int, isMemory bool) string {
	mode := ""
	if isMemory {
		mode = "&mode=memory"
	}
	return fmt.Sprintf(`attach database "file:%s/%s.db?cache=shared%s" as "%s";`, baseDir, GetDatabaseName(database), mode, GetDatabaseName(database))
}

func CreateDatabase(basePath string, databases []int, memoryDatabase []int) (*sql.DB, error) {

	baseDir := basePath
	if strings.HasSuffix(strings.ToLower(baseDir), ".db") {
		baseDir = path.Dir(basePath)
	}
	err := os.MkdirAll(baseDir, 0750)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite3", baseDir+"?cache=shared&mode=memory")
	if err != nil {
		return db, err
	}

	for _, diskdb := range databases {
		_, err := db.Exec(GetAttachDatabaseSql(baseDir, diskdb, false))
		if err != nil {
			return db, err
		}
	}

	for _, memdb := range memoryDatabase {
		_, err := db.Exec(GetAttachDatabaseSql(baseDir, memdb, true))
		if err != nil {
			return db, err
		}
	}

	return db, nil
}

func createTable(db *sql.DB, database, tableName int, columns []int, numColumns []int, keyColumns []int, indexColumns []int) {
	return
}
