package main

import (
	"log"
	"strconv"
	"strings"
)

func main() {

	const xx = `音效帅 is a good man`

	ret := strings.Map(func(x rune) rune {
		str := strconv.QuoteRune(x)
		log.Printf(str)
		if str == "s" {
			return rune('a')
		} else {
			return x
		}
	}, xx)
	log.Printf("%s", ret)
}
