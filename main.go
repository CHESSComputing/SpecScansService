package main

import (
	"log"

	srvConfig "github.com/CHESSComputing/golib/config"
)

func main() {
	srvConfig.Init()
	if srvConfig.Config.SpecScans.WebServer.Verbose > 0 {
		log.SetFlags(log.LstdFlags | log.Lshortfile)
	}

	Server()
}
