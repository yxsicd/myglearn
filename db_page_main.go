package main

import (
	"fmt"

	"github.com/tidwall/buntdb"
	"time"
)

func main() {
	db, _ := buntdb.Open(":memory:")

	i_time := time.Now()
	count := 100000
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
		c_time1 := time.Now()
		count, _ = tx.Len()
		c_time2 := time.Now()
		fmt.Printf("db len is %v,use time is %v\n", count, c_time2.Sub(c_time1))

		c_time3 := time.Now()
		sumc := 0
		tx.AscendKeys("*", func(key, value string) bool {
			sumc++
			return true
		})
		c_time4 := time.Now()
		fmt.Printf("db len is %v,use time is %v\n", sumc, c_time4.Sub(c_time3))

		c_time5 := time.Now()
		page_size := 10
		page_count := sumc / page_size

		for p := 0; p < page_count; p++ {

			if p%500 != 0 {
				continue
			}
			c_time7 := time.Now()
			pageGroup := make([]string, 0)
			pindex := 0
			p_start := p * page_size
			p_end := p*page_size + page_size
			tx.AscendKeys("*", func(key, value string) bool {
				pindex++
				if pindex >= p_start {
					pageGroup = append(pageGroup, key)
				}
				if pindex >= p_end {
					return false
				} else {
					return true
				}
			})
			c_time8 := time.Now()
			fmt.Printf("page_size %v, page_count %v, pageGroup is %v,use time is %v\n", page_size, page_count, pageGroup, c_time8.Sub(c_time7))

		}

		c_time6 := time.Now()
		fmt.Printf("page_size %v, page_count %v,use time is %v\n", page_size, page_count, c_time6.Sub(c_time5))

		return nil
	})
	db.Shrink()
}
