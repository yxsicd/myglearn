package main

import (
	"io/ioutil"

	"github.com/tidwall/evio"
)

func main() {
	var events evio.Events
	events.Data = func(id int, in []byte) (out []byte, action evio.Action) {
		out = append([]byte("return: "), in...)
		return
	}

	ioutil.WriteFile("test/111.222", []byte("xxxxxxxxxxxxxxx"), 0666)

	if err := evio.Serve(events, "tcp://localhost:5000"); err != nil {
		panic(err.Error())
	}

}
