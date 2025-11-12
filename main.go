package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tamararankovic/hidera/config"
	"github.com/tamararankovic/hidera/peers"
)

func main() {
	time.Sleep(15 * time.Second)

	conf := config.LoadFromEnv()
	peers, err := peers.NewPeers(conf)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println(conf)
	log.Println(peers.Peers)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	<-quit

	log.Println("Received shutdown signal...")
}
