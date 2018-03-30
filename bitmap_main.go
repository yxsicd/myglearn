package main

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"log"
	"math"
	"time"
)

type xx interface {
	Eat(string)
}

type fish struct {
}

type bear struct {
	Name string
}

func (*fish) Eat(food string) {
	log.Printf("fish eat %s", food)
}

func (*bear) Eat(food string) {
	log.Printf("bear %s", food)
}

func checkType(inputtype interface{}) {
	switch r1 := inputtype.(type) {
	case io.Reader:
		log.Printf("1111r1 is %T, r2 is %v", r1, inputtype)
	default:
		log.Printf("222r1 is %, r2 is %v", r1, inputtype)
	}
}

func MyTest() {
	fish := new(fish)
	fish.Eat("sadfsadf")

	bear := new(bear)
	bear.Name = "123"
	bear.Eat("sadfsadf")

	checkType(bear)
}

func gethash(value uint64) uint32 {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, value)
	return crc32.ChecksumIEEE(b)
}

var (
	ST, _    = time.Parse(time.RFC3339, "2017-01-01T00:00:00Z")
	now_time time.Time
	now_que  uint64
)

func idg() uint64 {
	dt := time.Now()
	use_time := dt.Sub(ST)
	dt_long := use_time.Nanoseconds() / int64(time.Millisecond)
	if (dt.UnixNano() / int64(time.Millisecond)) == (now_time.UnixNano() / int64(time.Millisecond)) {
		now_que++
		if now_que > 1<<12 {
			time.Sleep(time.Millisecond)
		}
	} else {
		now_time = dt
		now_que = 0
	}
	// log.Printf("time is %v, mise %v, day is %v", use_time, dt_long, dt_long/1000/60/60/24) //4923023361179653,,,

	retid := uint64(dt_long<<17) | (1 << 12) | (now_que)
	return retid
}

func splitlong(oldvalue uint64) (int, int) {
	// timevalue := oldvalue >> 17

	// bs := make([]byte, 8)
	// binary.LittleEndian.PutUint64(bs, oldvalue)
	// length := len(bs)
	// time_slice := bs[0:41]
	// // mache_code := bs[41:46]
	// que_code := bs[46:58]

	time_value := oldvalue >> 17
	que_value := (oldvalue << 48) >> 48
	value := (time_value << 12) | que_value

	//4923231313989831,
	// value_slice := append(time_slice, que_code...)
	// value := binary.LittleEndian.Uint64(value_slice)
	span := int(value / math.MaxInt32)
	offset := int(value % uint64(math.MaxInt32))
	return span, offset
}

func main() {
	log.Printf("int32 max is %v, uint32 max is %v, max quese is %v", math.MaxInt32, math.MaxUint32, 1<<12)
	log.Printf("max time %v, max long is %v, max span is %v", ST.Add(1<<41*time.Millisecond), 1<<41, 1<<41/math.MaxInt32)

	// log.Printf("long is %v, span is %v, offset is %v", 1<<41, s, o)
	for i := 0; i < 100; i++ {
		s, o := splitlong(idg())
		log.Printf("id is %v, span is %v, offset is %v", idg(), s, o)
	}
	// MyTest()
	// r := roaring.NewBitmap()
	// r2 := roaring.NewBitmap()

	// rand.Seed(123123123123)
	// for i := 0; i < 1000000; i++ {
	// 	r.AddInt(rand.Int())
	// 	r2.AddInt(rand.Int())
	// }
	// log.Printf("r size is %v", r.GetCardinality())
	// log.Printf("r s size  is %v", r.GetSerializedSizeInBytes())

	// log.Printf("r2 size is %v", r2.GetCardinality())
	// log.Printf("r2 s size  is %v", r2.GetSerializedSizeInBytes())

	// r3 := roaring.And(r, r2)
	// log.Printf("r3 size is %v", r3.GetCardinality())
	// log.Printf("r3 s size  is %v", r3.GetSerializedSizeInBytes())

}
