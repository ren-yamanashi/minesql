package main

import (
	"flag"
	"log"
	"minesql/internal/server"
	"os"
)

func main() {
	var (
		h            = flag.String("h", "localhost", "server host")
		p            = flag.Int("p", 8888, "server port")
		initUser     = flag.String("init-user", "", "initial username (required on first startup)")
		initPassword = flag.String("init-password", "", "initial password (required on first startup)")
		initHost     = flag.String("init-host", "", "initial allowed host (required on first startup)")
	)
	flag.Parse()

	// --init-* は 3 つセットで指定する必要がある
	initFlags := []*string{initUser, initPassword, initHost}
	specifiedCount := 0
	for _, f := range initFlags {
		if *f != "" {
			specifiedCount++
		}
	}
	if specifiedCount > 0 && specifiedCount < 3 {
		log.Fatal("--init-user, --init-password, --init-host must all be specified together")
	}

	var initOpts *server.InitUserOpts
	if specifiedCount == 3 {
		initOpts = &server.InitUserOpts{
			Username: *initUser,
			Password: *initPassword,
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
