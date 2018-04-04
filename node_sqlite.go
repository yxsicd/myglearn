package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path"
	"strings"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	// initdb("ttt.db")
	initHttp()
}

var dbmap = make(map[string]*sql.DB)
var dblock sync.RWMutex
var sqllock sync.RWMutex
var allrequest_count = 0

func getDB(dbname string) *sql.DB {
	dblock.RLock()
	db, ok := dbmap[dbname]
	dblock.RUnlock()
	if ok {
		return db
	} else {
		dblock.Lock()
		newdb := initdb(dbname)
		dbmap[dbname] = newdb
		dblock.Unlock()
		return newdb
	}
}

func initdb(dbname string) *sql.DB {
	// basepath := "/dev/shm"
	basepath := "./target/"
	dbpath := path.Join(basepath, dbname)
	// os.Remove(dbpath)
	db, err := sql.Open("sqlite3", dbpath+"?cache=shared&mode=rwc")
	// db, err := sql.Open("sqlite3", dbpath)

	if err != nil {
		log.Print(err)
	}
	db.Exec("PRAGMA journal_mode=WAL;")
	return db
}

func ExecDBSql(dbname string, sql string, jsondata string) (sql.Result, error) {
	sqllock.Lock()
	// exec_b := time.Now()
	db := getDB(dbname)
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt1, err := tx.Prepare(sql)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt1.Close()
	if jsondata != "" {
		var data [][]interface{}
		json.Unmarshal([]byte(jsondata), &data)

		for i := 0; i < len(data); i++ {
			value_list := data[i]
			_, err := stmt1.Exec(value_list[:]...)
			if err != nil {
				return nil, err
			}
		}
	} else {
		_, err := stmt1.Exec()
		if err != nil {
			return nil, err
		}
	}
	tx.Commit()
	// log.Printf("exec sql is %v, use time is %v", sql, time.Since(exec_b))
	sqllock.Unlock()
	return nil, nil
}

func QueryDBSql(dbname string, sql string) (*sql.Rows, error) {
	return getDB(dbname).Query(sql)
}

func initHttp() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		// begin_time := time.Now()
		qv := req.URL.Query()
		dbname := qv.Get("db")
		sqlstring := qv.Get("sql")

		if dbname == "" || sqlstring == "" {
			return
		}

		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(sqlstring)), "select") {
			rows, err := QueryDBSql(dbname, sqlstring)
			if err != nil {
				fmt.Fprintf(w, "%s", err)
				return
			}

			columns, err := rows.Columns()
			if err != nil {
				fmt.Fprintf(w, "%s", err)
				return
			}
			column_count := len(columns)
			for rows.Next() {
				value_list := make([]interface{}, 0)
				for j := 0; j < column_count; j++ {
					var v string
					value_list = append(value_list, &v)
				}
				err = rows.Scan(value_list...)
				if err != nil {
					log.Fatal(err)
				}
				for _, v := range value_list {

					vs, ok := v.(*string)
					if ok {
						fmt.Fprintf(w, "%s,", *vs)
					}
				}
				fmt.Fprintf(w, "\n")
			}
			allrequest_count++
			if allrequest_count%50 == 0 {
				log.Printf("all request is %v", allrequest_count)
			}
			return
		} else {
			// exec_begin_time := time.Now()
			sqldata := qv.Get("data")
			_, err := ExecDBSql(dbname, sqlstring, sqldata)
			if err != nil {
				fmt.Fprintf(w, "%s", err)
			}
			allrequest_count++
			if allrequest_count%50 == 0 {
				log.Printf("all request is %v", allrequest_count)
			}

			// log.Printf("exec sql use time is %v, all time is %v", time.Since(exec_begin_time), time.Since(begin_time))
			return
		}

	})
	http.ListenAndServe(":8080", mux)
}
