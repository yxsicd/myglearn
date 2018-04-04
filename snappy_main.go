package main

import (
	"log"

	"github.com/golang/snappy"
)

func main() {
	srcByte := []byte("asadfsadfaaaaaaaaaaaaaaaaasadfasfasfddfasdfdddddssssssssssssssddddddddddddddasdf")
	compress_byte := snappy.Encode(nil, srcByte)
	decompress_byte, _ := snappy.Decode(nil, compress_byte)
	log.Printf("compress %s, src length is %v, compress length is %v", decompress_byte, len(srcByte), len(compress_byte))
}
