package main

import "flag"

func main() {
  var config string
  flag.StringVar(&config, "config", "serverconfig_test.json", "Server config JSON file")
  flag.Parse()
  Server(config)
}
