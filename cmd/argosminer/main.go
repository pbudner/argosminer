package main

import "C"
import (
	"github.com/pbudner/argosminer/deamon"
	"github.com/webview/webview"
)

var (
	GitCommit = "live"
	Version   = ""
)

func main() {
	d := deamon.NewDeamon()
	go d.Run()

	w := webview.New(false)
	defer w.Destroy()
	w.SetTitle("ArgosMiner - Analyzing Process Dynamics")
	w.SetSize(1200, 800, webview.HintNone)
	w.Navigate("http://localhost:4711")
	w.Run()

	// close deamon
	d.Close()
}
