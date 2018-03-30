package main

import "fmt"
import (
	"bytes"
	"github.com/cznic/ql"
	"time"
)

func main() {

	db, err := ql.OpenMem()
	if err != nil {
		panic(err)
	}

	db.Run(ql.NewRWCtx(), `
    BEGIN TRANSACTION;
        CREATE TABLE t (i int, s string,s2 string, s3 string);
    COMMIT;
`,
	)

	start := time.Now()
	var buffer bytes.Buffer
	buffer.Write([]byte("BEGIN TRANSACTION;"))
	for i := 0; i < 100000; i++ {
		sql := fmt.Sprintf(`INSERT INTO t VALUES (%v, "%v","%v","%v");`, i, i, i, i)
		buffer.Write([]byte(sql))
	}
	buffer.Write([]byte("COMMIT;"))
	end1 := time.Now()
	fmt.Printf("make sql time is %v\n", end1.Sub(start))
	db.Run(ql.NewRWCtx(), buffer.String())

	end2 := time.Now()
	fmt.Printf("run sql time is %v\n", end2.Sub(end1))

	rss, _, err := db.Run(ql.NewRWCtx(), `
    SELECT count(1) FROM t ;`,
	)
	if err != nil {
		panic(err)
	}

	for _, rs := range rss {
		if err := rs.Do(false, func(data []interface{}) (bool, error) {
			fmt.Println(data)
			return true, nil
		}); err != nil {
			panic(err)
		}
		fmt.Println("----")
	}

}
