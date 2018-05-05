package yxsdb

import (
	"database/sql"
	"fmt"
	"os"
	"path"
	"strings"

	_ "github.com/mattn/go-sqlite3"
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
		databaseList = append(databaseList, database)
	}
	return databaseList
}

func GetDatabaseMap(db *sql.DB) map[string]string {
	retmap := make(map[string]string)
	dbList := GetDatabaseList(db)
	for _, db := range dbList {
		retmap[db.Name] = db.FilePath
	}
	return retmap
}

func isAttachedDatabase(db *sql.DB, database int) bool {
	retMap := GetDatabaseMap(db)
	_, ok := retMap[GetDatabaseName(database)]
	return ok
}

func GetAttachDatabaseSQL(baseDir string, database int, isMemory bool) string {
	mode := ""
	if isMemory {
		mode = "&mode=memory"
	}
	return fmt.Sprintf(`attach database "file:%s/%s.db?cache=shared%s" as "%s";`, baseDir, GetDatabaseName(database), mode, GetDatabaseName(database))
}

func GetDetachDatabaseSQL(database int) string {
	return fmt.Sprintf(`detach database "%s";`, GetDatabaseName(database))
}

func AttachDatabase(basePath string, db *sql.DB, database int, isMemory bool) error {
	if !isAttachedDatabase(db, database) {
		_, err := db.Exec(GetAttachDatabaseSQL(basePath, database, isMemory))
		if err != nil {
			return err
		}
	}
	return nil
}

func DetachDatabase(db *sql.DB, database int) error {
	if isAttachedDatabase(db, database) {
		_, err := db.Exec(GetDetachDatabaseSQL(database))
		if err != nil {
			return err
		}
	}
	return nil
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

	db, err := sql.Open("sqlite3", "file:"+baseDir+"?cache=shared&mode=memory")
	if err != nil {
		return db, err
	}

	for _, diskdb := range databases {
		_, err := db.Exec(GetAttachDatabaseSQL(baseDir, diskdb, false))
		if err != nil {
			return db, err
		}
	}

	for _, memdb := range memoryDatabase {
		_, err := db.Exec(GetAttachDatabaseSQL(baseDir, memdb, true))
		if err != nil {
			return db, err
		}
	}

	return db, nil
}

func GetColumnName(column int) string {
	return fmt.Sprintf("_%v", column)
}

func GetTableColumnName(tableName int, column int) string {
	return fmt.Sprintf("%s.%s", GetTableName(tableName), GetColumnName(column))
}

func GetDatabaseTableColumnName(database int, tableName int, column int) string {
	return fmt.Sprintf(`%s."%s"`, GetDatabaseName(database), GetTableColumnName(tableName, column))
}

func GetColumnDefine(column int, columnType string) string {
	return fmt.Sprintf("%s %s", GetColumnName(column), columnType)
}

func GetCreateTableSQL(database, tableName int, columns []int, numberColumnMap map[int]string, keyColumnMap map[int]string) string {
	columnName := make([]string, 0)
	for _, column := range columns {
		columnDefine := GetColumnDefine(column, "text")
		if _, ok := numberColumnMap[column]; ok {
			columnDefine = GetColumnDefine(column, "num")
		}

		if _, ok := keyColumnMap[column]; ok {
			columnDefine = columnDefine + " primary key "
		}
		columnName = append(columnName, columnDefine)
	}

	columnSQL := strings.Join(columnName, ",")
	createSQL := fmt.Sprintf(`create table if not exists %s  ( %s );`, GetDatabaseTableName(database, tableName), columnSQL)

	return createSQL
}

func GetCreateTableIndexSQL(database, tableName int, index int) string {
	return fmt.Sprintf(`create index if not exists %s on %s ( %s );`, GetDatabaseTableColumnName(database, tableName, index), GetTableName(tableName), GetColumnName(index))
}

func GetInitTableSQL(database, tableName int, columns []int, numberColumnMap map[int]string, keyColumnMap map[int]string, indexColumns []int) string {

	runSQL := ""
	tableInitSQL := GetCreateTableSQL(database, tableName, columns, numberColumnMap, keyColumnMap)
	runSQL += tableInitSQL + "\n"
	for _, v := range indexColumns {
		runSQL += GetCreateTableIndexSQL(database, tableName, v) + "\n"
	}
	return runSQL
}

func InitTable(db *sql.DB, database, tableName int, columns []int, numberColumnMap map[int]string, keyColumnMap map[int]string, indexColumns []int) error {
	runSQL := GetInitTableSQL(database, tableName, columns, numberColumnMap, keyColumnMap, indexColumns)
	_, err := db.Exec(runSQL)
	return err
}
