package main

import (
	"fmt"

	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
	"strings"
	"time"
)

func main() {
	db, _ := buntdb.Open(":memory:")

	i_time := time.Now()
	count := 20000
	db.Update(func(tx *buntdb.Tx) error {
		for i := 0; i < count; i++ {
			tx.Set(fmt.Sprint(i), fmt.Sprintf(`{"name":{"first":"Tom","last":"John%vson"},"age":38}`, i), nil)
		}
		return nil
	})
	ie_time := time.Now()
	fmt.Println("insert time use %v", ie_time.Sub(i_time))
	db.CreateIndex("last_name", "*", buntdb.IndexJSON("name.last"))
	ie_time2 := time.Now()
	fmt.Println("last_name index time use %v", ie_time2.Sub(ie_time))
	db.CreateIndex("age", "*", buntdb.IndexJSON("age"))
	ie_time3 := time.Now()
	fmt.Println("age index time use %v", ie_time3.Sub(ie_time2))

	db.Update(func(tx *buntdb.Tx) error {
		for i := count; i < count*2; i++ {
			tx.Set(fmt.Sprint(i), fmt.Sprintf(`{"name":{"first":"Tom","last":"John%vson"},"age":38}`, i), nil)
		}
		return nil
	})
	ie_time4 := time.Now()
	fmt.Println("insert again time use %v", ie_time4.Sub(ie_time3))

	db.View(func(tx *buntdb.Tx) error {
		fmt.Println("Order by last name")
		q_time11 := time.Now()
		tx.Ascend("last_name", func(key, value string) bool {
			//fmt.Printf("%s: %s\n", key, value)
			return true
		})

		q_time21 := time.Now()
		fmt.Println("query21 time use %v", q_time21.Sub(q_time11))
		fmt.Println("Order by age")
		tx.Ascend("age", func(key, value string) bool {
			//fmt.Printf("%s: %s\n", key, value)
			return true
		})
		q_time22 := time.Now()
		fmt.Println("query22 time use %v", q_time22.Sub(q_time21))

		q_time1 := time.Now()
		fmt.Println("Order by age range 30-50")
		rets := make([]string, 0)
		tx.Ascend("last_name", func(key, value string) bool {
			ret := strings.Contains(gjson.Get(value, "name.last").String(), "4564")
			if ret {
				rets = append(rets, fmt.Sprintf("%s: %s\n", key, value))
			}
			return true
		})
		l_count := 0
		tx.AscendKeys("*", func(key, value string) bool {
			l_count++
			return true
		})
		fmt.Printf("count=%s", l_count)
		q_time2 := time.Now()
		fmt.Println("query time use %v", q_time2.Sub(q_time1))
		fmt.Printf("rets is %v", len(rets))
		return nil
	})
	db.Shrink()
}
