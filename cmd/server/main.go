package main

import (
	"flag"
	"log"
	"minesql/internal/server"
	"os"
)

func main() {
	var (
		h        = flag.String("h", "localhost", "server host")
		p        = flag.Int("p", 8888, "server port")
		initUser = flag.String("init-user", "", "initial username (required on first startup)")
		initHost = flag.String("init-host", "", "initial allowed host (required on first startup)")
	)
	flag.Parse()

	// --init-user と --init-host はセットで指定する必要がある
	userSpecified := *initUser != ""
	hostSpecified := *initHost != ""
	if userSpecified != hostSpecified {
		log.Fatal("--init-user and --init-host must be specified together")
	}

	var initOpts *server.InitUserOpts
	if userSpecified {
		initOpts = &server.InitUserOpts{
			Username: *initUser,
			Host:     *initHost,
		}
	}

	sv := server.NewServer(*h, *p, initOpts)
	sd := server.NewShutdown()

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
