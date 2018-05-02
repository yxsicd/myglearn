package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func testinsert(i_count int, b_count int, dbname string, c_count int, db_index int) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Llongfile)

	// basepath := "/dev/shm"
	basepath := "./target/"
	// filebasepath := "./target/"

	dbpath := path.Join(basepath, dbname)
	// filedbpath := path.Join(filebasepath, dbname)

	os.Remove(dbpath)
	// os.Remove(filedbpath)

	db, err := sql.Open("sqlite3", dbpath+"?cache=shared&mode=rwc")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	// db.Exec("PRAGMA journal_mode=WAL;")
	column_count := c_count
	column_name := make([]string, 0)
	value_name := make([]string, 0)
	for i := 0; i < column_count; i++ {
		column_name = append(column_name, fmt.Sprintf("_%v", i))
		value_name = append(value_name, fmt.Sprintf("?"))
	}

	sqlStmt := fmt.Sprintf(`
	create table _0 ( %s );
	attach database ":memory:" as "mem";
	create table mem._2 ( %s );
	delete from _0;
	delete from mem._2;
	create index id0_index on _0(_0,_1);
	create index mem.id2_index on _2(_0,_1);
		
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
	batch_count := b_count
	insert_count := i_count
	for b := 0; b < batch_count; b++ {
		batch_values = append(batch_values, fmt.Sprintf("(%s)", cvalue))
	}
	batch_all_values := strings.Join(batch_values, ",")

	psql_1 := fmt.Sprintf("insert into mem._2 (%s) values %s ;", cname, batch_all_values)
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

	all_time := float64(0)

	for i := 0; i < insert_count; i++ {
		value_list := make([]interface{}, 0)
		for b := 0; b < batch_count; b++ {
			ivalue := i*batch_count + b + db_index*(insert_count*batch_count)
			for j := 0; j < column_count; j++ {
				// value_list = append(value_list, fmt.Sprintf("value-%v-%v-%v", i, j, b))
				// ivalue := fmt.Sprintf("value-%v-%v-%v", i, j, b)

				value_list = append(value_list, ivalue)

				// value_list = append(value_list, i)
			}
		}
		exe_time := time.Now()
		_, err = stmt1.Exec(value_list[:]...)
		exe_use_time := time.Since(exe_time)
		all_time += float64(exe_use_time.Nanoseconds())
		if err != nil {
			log.Fatal(err)
		}
		// _, err = stmt2.Exec(value_list[:]...)
		// if err != nil {
		// 	log.Fatal(err)
		// }
	}
	insert_use := time.Since(insert_begin)
	// insert_use_time := float64(insert_use.Nanoseconds()) / 1000000000
	insert_use_time := all_time / 1000000000
	allinsert_count := insert_count * batch_count
	insert_speed := float64(allinsert_count) / insert_use_time
	log.Printf("end insert %s, row count is %v, use time is %v, insert_speed is %v", dbname, allinsert_count, insert_use, insert_speed)

	copy_begin := time.Now()
	stmt2, err := tx.Prepare("insert into _0 select * from mem._2;")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt2.Close()
	// stmt2.Exec()
	stmt2.Exec()
	tx.Commit()
	// targetdb, err := os.Create(filedbpath)
	// srcdb, err := os.Open(dbpath)
	// io.Copy(targetdb, srcdb)
	// srcdb.Close()
	// targetdb.Close()

	copy_time := time.Since(copy_begin)
	copy_use_time := float64(copy_time.Nanoseconds()) / 1000000000
	copy_count := insert_count * batch_count
	copy_speed := float64(copy_count) / copy_use_time

	log.Printf("end insert %s, row count is %v, use time is %v, copy time is %v, copy_speed is %v", dbname, copy_count, insert_use, copy_time, copy_speed)

}

func testquery(i_count int, b_count int, dbname string, c_count int, querySql string) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Llongfile)

	// basepath := "/dev/shm/"
	basepath := "./target/"

	dbpath := path.Join(basepath, dbname)

	// os.Remove(dbpath)
	db, err := sql.Open("sqlite3", dbpath+"?cache=shared&mode=rwc")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	// db.Exec("PRAGMA journal_mode=WAL;")
	// column_count := c_count
	// rows, err := db.Query("select * from _0;")
	// rows, err := db.Query("select * from _0 where _1 like '%381%' ")

	column_count := 1

	query_begin_time := time.Now()
	// rows, err := db.Query("select _1 from _0 where _3%11=1 and _3%13=4  and _1%4=1 order by _3%8  ")
	rows, err := db.Query(querySql)

	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	count := 0
	scan_begin_time := time.Now()
	// log.Printf("begin query row count is %v", count)
	for rows.Next() {
		// value_list := make([]interface{}, 0)
		// for j := 0; j < column_count; j++ {
		// 	var v string
		// 	value_list = append(value_list, &v)
		// }
		// err = rows.Scan(value_list...)
		// if err != nil {
		// 	log.Fatal(err)
		// }
		count++
	}
	log.Printf("column count is %v, scan query %s, row count is %v, scan use time is %v, query use time is %v", column_count, dbname, count, time.Since(scan_begin_time), scan_begin_time.Sub(query_begin_time))

	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
}

func testp(t_count int, pcount int) {
	lockchan := make(chan int, 4)

	p_count := pcount
	i_count := t_count / p_count
	b_count := 10
	c_count := 30
	all_count := float64(p_count * i_count * b_count)

	begin_time := time.Now()
	for i := 0; i < p_count; i++ {
		go func(dbindex int) {
			testinsert(i_count, b_count, fmt.Sprintf("%v.db", dbindex), c_count, dbindex)
			lockchan <- 1
		}(i)
	}

	for i := 0; i < p_count; i++ {
		<-lockchan
	}

	user_time := float64(time.Since(begin_time).Nanoseconds()) / 1000000000
	log.Printf("---------------------------------\n")
	log.Printf("%v insert done!, all insert count is %v, %v", p_count, all_count, all_count/user_time)

	begin_time_query := time.Now()
	for i := 0; i < p_count; i++ {
		go func(dbindex int) {
			testquery(i_count, b_count, fmt.Sprintf("%v.db", dbindex), c_count, "select * from _0 limit 10")
			lockchan <- 1
		}(i)
	}

	for i := 0; i < p_count; i++ {
		<-lockchan
	}
	all_query_time := time.Since(begin_time_query)
	user_time = float64(all_query_time.Nanoseconds()) / 1000000000
	log.Printf("%v query done!, all query count is %v, speed is %v, use time is %v", p_count, all_count, all_count/user_time, all_query_time)
}

func testpq(t_count int, pcount int, querySql string) {
	lockchan := make(chan int, pcount)

	p_count := pcount
	i_count := t_count / p_count
	b_count := 10
	c_count := 30
	all_count := float64(p_count * i_count * b_count)

	begin_time_query := time.Now()
	for i := 0; i < p_count; i++ {
		go func(dbindex int, querySql string) {
			testquery(i_count, b_count, fmt.Sprintf("%v.db", dbindex), c_count, querySql)
			lockchan <- 1
		}(i, querySql)
	}

	for i := 0; i < p_count; i++ {
		<-lockchan
	}
	all_query_time := time.Since(begin_time_query)
	user_time := float64(all_query_time.Nanoseconds()) / 1000000000
	log.Printf("%v query done!, all query count is %v, speed is %v, use time is %v", p_count, all_count, all_count/user_time, all_query_time)
}

func main() {
	mode := flag.String("m", "r", "mode")
	sql := flag.String("s", "select * from _0 limit 100", "sql")
	threadCount := flag.Int("t", 4, "query ThreadCount")
	allrowCount := flag.Int("r", 144000, "allrowCount")

	flag.Parse()
	// for i := 1; i < 9; i++ {
	// 	testp(16000, i)
	// }

	// testp(144000, 1)
	// testp(144000, 3)
	// testp(14400, 64)
	if *mode == "w" {
		testp(*allrowCount, *threadCount)
	} else {
		testpq(*allrowCount, *threadCount, *sql)
	}

}
