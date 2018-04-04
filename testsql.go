package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"
)

/*

function reqListener() {//   console.log(this.responseText);
}

function sendsql(db, sql) {
    var oReq = new XMLHttpRequest();
    oReq.onload = reqListener;
    sqle = encodeURIComponent(sql);
    urls = `?db=${db}&sql=${sqle}`
    oReq.open("get", urls, true);
    oReq.send();
}

for (var i = 0; i < 100000; i++) {
    sendsql("test.db", "select count(1) from people");
}



*/

var all_ret_len int64

func sendsql(db, sql string, data [][]string, show bool) {
	esql := url.QueryEscape(sql)
	edb := url.QueryEscape(db)

	ejsondata := ""
	if data != nil {
		jsondata, _ := json.Marshal(data)
		ejsondata = url.QueryEscape(string(jsondata))
	}
	ret, err := http.Get(fmt.Sprintf("http://localhost:8080/?db=%s&sql=%s&data=%s", edb, esql, ejsondata))
	if err != nil {
		log.Printf("send sql err is %s", err)
		return
	}
	defer ret.Body.Close()
	retb, err := ioutil.ReadAll(ret.Body)
	if err != nil {
		log.Printf("sql run err is %s", err)
		return
	}
	all_ret_len = int64(len(retb)) + all_ret_len
	if show {
		log.Printf("run sql %s ret  is %s", sql, retb)
	}
}

func batchsql(sql string, data [][]string, t_count int) {
	batchsqlshow(sql, data, t_count, false)
}

func batchsqlshow(sql string, data [][]string, t_count int, show bool) {
	begin_time := time.Now()
	done := make(chan int, t_count)
	p_count := make(chan int, 256)
	for i := 0; i < t_count; i++ {
		go func() {
			p_count <- 1
			sendsql("test.db", sql, data, show)
			<-p_count
			done <- 1
		}()
	}
	for i := 0; i < t_count; i++ {
		<-done
	}

	use_time := time.Since(begin_time)
	speed := float64(t_count) / (float64(use_time.Nanoseconds()) / 1000000000)
	log.Printf("run %s use time is %v, speed is %v, all_ret_content is %v, one requeset use time is %v", sql,
		use_time, speed, all_ret_len/1024, use_time.Nanoseconds()/int64(t_count))
}

func main() {

	var batch_sql []string
	for i := 0; i < 300; i++ {
		batch_sql = append(batch_sql, "insert into people select 'n1','123123123';")
	}
	batchsql("create table if not exists people(name,age);", nil, 1)
	batchsql("create index if not exists people_index on people(name,age);", nil, 1)
	batchsqlshow("PRAGMA '.schema';", nil, 1, true)
	// batchsql(strings.Join(batch_sql, " "), 10)

	var batch_insert_data [][]string
	for i := 0; i < 10000; i++ {
		var insert_row []string
		insert_row = append(insert_row, fmt.Sprintf("%v", time.Now().UnixNano()), fmt.Sprintf("%v", time.Now().UnixNano()))
		batch_insert_data = append(batch_insert_data, insert_row)
	}
	batchsql("insert into people values(?,?);", batch_insert_data, 1)
	batchsql("select * from people where name in('1522711842995173377') limit 1;", nil, 1000)
	batchsqlshow("select * from people where name in('1522711842995173377') limit 1;", nil, 1, true)

	batchsqlshow("select name,age, name%1024 from people where name%1024=888 limit 1;", nil, 1, true)
	batchsqlshow("select count(1) from people where instr(name, '1522711842995')>0 ", nil, 3, true)

	// batchsql("select count(1) from people ;", nil, 1000)
	batchsqlshow("select count(1) from people ;", nil, 1, true)

}
