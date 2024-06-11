package main

import (
	srvConfig "github.com/CHESSComputing/golib/config"
)

func main() {
	srvConfig.Init()

	Server()
}
