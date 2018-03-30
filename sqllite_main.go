package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func testinsert(insertcount int, dbname string) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Llongfile)

	// basepath := "/dev/shm"
	basepath := "./test/"

	dbpath := path.Join(basepath, dbname)

	os.Remove(dbpath)
	db, err := sql.Open("sqlite3", dbpath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	column_count := 50
	column_name := make([]string, 0)
	value_name := make([]string, 0)
	for i := 0; i < column_count; i++ {
		column_name = append(column_name, fmt.Sprintf("_%v", i))
		value_name = append(value_name, fmt.Sprintf("?"))
	}

	sqlStmt := fmt.Sprintf(`
	create table _0 ( %s );
	create table _2 ( %s );
	delete from _0;
	delete from _2;
	
	`, strings.Join(column_name, ","), strings.Join(column_name, ","))
	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	cname := strings.Join(column_name, ",")
	cvalue := strings.Join(value_name, ",")

	batch_values := make([]string, 0)
	batch_count := 5
	insert_count := insertcount
	for b := 0; b < batch_count; b++ {
		batch_values = append(batch_values, fmt.Sprintf("(%s)", cvalue))
	}
	batch_all_values := strings.Join(batch_values, ",")

	psql_1 := fmt.Sprintf("insert into _0 (%s) values %s ;", cname, batch_all_values)
	// psql_2 := fmt.Sprintf("insert into _2 (%s) values %s ;", cname, batch_all_values)
	// log.Printf("psql is %s", psql_1)
	stmt1, err := tx.Prepare(psql_1)
	// stmt2, err := tx.Prepare(psql_2)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt1.Close()

	insert_begin := time.Now()
	// log.Printf("begin insert row count is %v", insert_count)

	for i := 0; i < insert_count; i++ {
		value_list := make([]interface{}, 0)
		for j := 0; j < column_count; j++ {
			for b := 0; b < batch_count; b++ {
				value_list = append(value_list, fmt.Sprintf("value-%v-%v-%v", i, j, b))
			}
		}

		_, err = stmt1.Exec(value_list[:]...)
		if err != nil {
			log.Fatal(err)
		}
		// _, err = stmt2.Exec(value_list[:]...)
		// if err != nil {
		// 	log.Fatal(err)
		// }
	}
	tx.Commit()
	log.Printf("end insert %s, row count is %v, use time is %v", dbname, insert_count, time.Since(insert_begin))

	rows, err := db.Query("select * from _0")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	count := 0
	query_time := time.Now()
	// log.Printf("begin query row count is %v", count)
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
		if count == 0 {
			// for i, v := range value_list {
			// 	// mv := (*v).(string)
			// 	// fmt.Printf("i=%v,v=%s ", i, *(v.(*string)))
			// }
		}
		count++
	}
	log.Printf("end query %s, row count is %v, query use time is %v", dbname, count, time.Since(query_time))

	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

}

func main() {
	lockchan := make(chan int, 4)

	p_count := 6

	for i := 0; i < p_count; i++ {
		go func(dbindex int) {
			testinsert(4000, fmt.Sprintf("%v.db", dbindex))
			lockchan <- 1
		}(i)
	}

	for i := 0; i < p_count; i++ {
		<-lockchan
	}
	log.Printf("insert done!")
}
