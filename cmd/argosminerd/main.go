package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/labstack/gommon/log"
	"github.com/pbudner/argosminer/deamon"
)

var (
	GitCommit = "live"
	Version   = ""
)

func main() {
	d := deamon.NewDeamon()
	go d.Run()

	// wait here before closing all workers
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-termChan // Blocks here until interrupted
	log.Info("SIGTERM received, initiating shutdown now")
	d.Close()
}
