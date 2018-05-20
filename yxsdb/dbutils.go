package yxsdb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strconv"
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

func InitAttachDatabase(basePath string, db *sql.DB, databases []int, memoryDatabase []int) (*sql.DB, error) {
	baseDir := basePath
	if strings.HasSuffix(strings.ToLower(baseDir), ".db") {
		baseDir = path.Dir(basePath)
	}
	err := os.MkdirAll(baseDir, 0750)
	if err != nil {
		return nil, err
	}

	for _, diskdb := range databases {
		err := AttachDatabase(baseDir, db, diskdb, false)
		if err != nil {
			return db, err
		}
	}

	for _, memdb := range memoryDatabase {
		err := AttachDatabase(baseDir, db, memdb, true)
		if err != nil {
			return db, err
		}
	}
	return db, nil
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
	return InitAttachDatabase(basePath, db, databases, memoryDatabase)
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

func GetPrepareValuesSQL(columnCount int) string {
	valueList := make([]string, 0)
	for i := 0; i < columnCount; i++ {
		valueList = append(valueList, fmt.Sprintf("?"))
	}
	return fmt.Sprintf("(%s)", strings.Join(valueList, ","))
}

func GetBatchPrepareValuesSQL(batchCount, columnCount int) string {
	valueList := make([]string, 0)
	for i := 0; i < batchCount; i++ {
		valueList = append(valueList, GetPrepareValuesSQL(columnCount))
	}
	return fmt.Sprintf(" %s ", strings.Join(valueList, ","))
}

func GetColumnsSQL(columns []int) string {
	valueList := make([]string, 0)
	for _, v := range columns {
		valueList = append(valueList, GetColumnName(v))
	}
	return fmt.Sprintf(" (%s) ", strings.Join(valueList, ","))
}

func GetSelectColumnsSQL(columns []int) string {
	valueList := make([]string, 0)
	for _, v := range columns {
		valueList = append(valueList, GetColumnName(v))
	}
	return fmt.Sprintf(" %s ", strings.Join(valueList, ","))
}

func GetInsertSQL(database, tableName, batchCount int, columns []int) string {
	return fmt.Sprintf("insert into %s %s values %s ", GetDatabaseTableName(database, tableName), GetColumnsSQL(columns), GetBatchPrepareValuesSQL(batchCount, len(columns)))
}

func batchInsertRows(tx *sql.Tx, database, tableName, batchCount int, columns []int, rows [][]interface{}) error {
	insertStmt, err := tx.Prepare(GetInsertSQL(database, tableName, batchCount, columns))
	if err != nil {
		return err
	}

	var batchRows []interface{}
	nextInsertIndex := 0
	for i, row := range rows {
		batchRows = append(batchRows, row...)
		if (i+1)%batchCount == 0 {
			_, err := insertStmt.Exec(batchRows...)
			if err != nil {
				return err
			}
			batchRows = batchRows[:0]
			nextInsertIndex = i + 1
		}
	}
	insertStmt.Close()
	if nextInsertIndex < len(rows) {
		// return nil
		return batchInsertRows(tx, database, tableName, 1, columns, rows[nextInsertIndex:])
	}
	return nil
}

func InsertRows(db *sql.DB, database, tableName int, columns []int, rows [][]interface{}) error {
	return BatchInsertRows(db, database, tableName, 5, columns, rows)
}

func BatchInsertRows(db *sql.DB, database, tableName, batchCount int, columns []int, rows [][]interface{}) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = batchInsertRows(tx, database, tableName, batchCount, columns, rows)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func ClearTable(db *sql.DB, database, tableName int) error {
	_, err := db.Exec(fmt.Sprintf("delete from %s", GetDatabaseTableName(database, tableName)))
	return err
}

type CacheTable struct {
	Columns       []int
	ColumnNames   []string
	Rows          [][]interface{}
	RowsShowCount int
}

func (table *CacheTable) String() string {

	lastRow := len(table.Rows)
	if table.RowsShowCount < lastRow {
		lastRow = table.RowsShowCount
	}

	showRows := table.Rows[:lastRow]
	var showStringRows [][]interface{}
	for _, row := range showRows {
		var stringRow []interface{}
		for _, cell := range row {
			showCell := cell
			cellPoint := *(cell.(*interface{}))
			bytea, ok := cellPoint.([]byte)
			if ok {
				showCell = fmt.Sprintf("%s", bytea)
			}
			stringRow = append(stringRow, showCell)
		}
		showStringRows = append(showStringRows, stringRow)
	}

	showRowsString, _ := json.Marshal(showStringRows)
	showColums, _ := json.Marshal(table.Columns)
	showColumnNames, _ := json.Marshal(table.ColumnNames)
	retString := fmt.Sprintf(`Columns=%s,ColumnNames=%s,Rows=%s`,
		showColums, showColumnNames, showRowsString)
	return retString
}

func (table *CacheTable) GetJSONTable() *CacheTable {
	showRows := table.Rows[:]
	var showStringRows [][]interface{}
	for _, row := range showRows {
		var stringRow []interface{}
		for _, cell := range row {
			showCell := cell
			cellPoint := *(cell.(*interface{}))
			bytea, ok := cellPoint.([]byte)
			if ok {
				showCell = fmt.Sprintf("%s", bytea)
			}
			stringRow = append(stringRow, showCell)
		}
		showStringRows = append(showStringRows, stringRow)
	}
	retCacheTable := CacheTable{Rows: showStringRows,
		ColumnNames:   table.ColumnNames,
		Columns:       table.Columns,
		RowsShowCount: table.RowsShowCount}
	return &retCacheTable
}

func ParseColumnName(index int, columnName string) int {
	intColumn := strings.Replace(columnName, "_", "", -1)
	s, err := strconv.Atoi(intColumn)
	if err != nil {
		return s
	} else {
		return index
	}
}

func QueryTable(db *sql.DB, querySQL string) (*CacheTable, error) {
	var retTable CacheTable
	rows, err := db.Query(querySQL)
	if err != nil {
		return &retTable, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return &retTable, err
	}

	for i, column := range columns {
		retTable.Columns = append(retTable.Columns, ParseColumnName(i, column))
		retTable.ColumnNames = append(retTable.ColumnNames, column)
	}

	for rows.Next() {
		valueList := make([]interface{}, 0)
		for _ = range columns {
			var v interface{}
			valueList = append(valueList, &v)
		}
		err = rows.Scan(valueList...)
		if err != nil {
			return &retTable, err
		}
		retTable.Rows = append(retTable.Rows, valueList)
	}
	return &retTable, nil
}
