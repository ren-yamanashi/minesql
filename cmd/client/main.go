package main

import (
	"flag"
	"minesql/internal/client"
)

func main() {
	var (
		h = flag.String("h", "localhost", "server host")
		p = flag.Int("p", 8888, "server port")
	)
	flag.Parse()
	cl := client.NewClient(*h, *p)
	err := cl.Start()
	if err != nil {
		panic(err)
	}
}
