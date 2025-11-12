package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tamararankovic/hidera/config"
	"github.com/tamararankovic/hidera/hidera"
	"github.com/tamararankovic/hidera/peers"
)

func main() {
	time.Sleep(7 * time.Second)

	params := config.LoadParamsFromEnv()
	conf := config.LoadConfigFromEnv()
	peers, err := peers.NewPeers(conf)
	if err != nil {
		log.Fatalln(err)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	hidera := hidera.NewHidera(params, peers)

	hidera.Run()

	<-quit

	log.Println("received shutdown signal...")
}
