package main

import (
	"flag"
	"minesql/internal/server"
)

func main() {
	var (
		h = flag.String("h", "localhost", "server host")
		p = flag.Int("p", 8888, "server port")
	)
	flag.Parse()
	sv := server.NewServer(*h, *p)
	err := sv.Start()
	if err != nil {
		panic(err)
	}
}
