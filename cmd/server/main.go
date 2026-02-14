package main

import (
	"flag"
	"log"
	"minesql/internal/server"
	"minesql/internal/shared"
	"os"
)

func main() {
	var (
		h = flag.String("h", "localhost", "server host")
		p = flag.Int("p", 8888, "server port")
	)
	flag.Parse()
	sv := server.NewServer(*h, *p)
	sd := shared.NewShutdown()

	// register shutdown hook
	err := sd.Add("server_shutdown", func(sig os.Signal) {
		log.Printf("Shutting down server due to signal: %v", sig)
		if err := sv.Stop(); err != nil {
			panic(err)
		}
	})
	if err != nil {
		panic(err)
	}

	// sd.Listen にブロックされないように別 goroutine でサーバーを起動
	go func() {
		if err := sv.Start(); err != nil {
			panic(err)
		}
	}()

	sd.Listen()
	log.Println("Server stopped.")
}
