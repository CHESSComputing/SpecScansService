package main

import (
  "fmt"
  "io/ioutil"
  "log"
  "encoding/json"
  "net/http"
  "strings"

  "github.com/gorilla/mux"
)

type Configuration struct {
  Port int    `json:"port"`
  Base string `json:"string"`
}

var Config Configuration

// Read configFile as JSON, place in "Config" variable
func ParseConfig(configFile string) {
  data, err := ioutil.ReadFile(configFile)
  if err != nil {
    log.Fatal("Unable to read config file %s, error %v", configFile, err)
  }
  err = json.Unmarshal(data, &Config)
  if err != nil {
    log.Fatalf("Unable to parse config file %s, error %v", configFile, err)
  }
}

// Helper to get full path names for api endpoints
func getPath(api string) string {
  base := Config.Base
  if base != "" {
    if ! strings.HasPrefix(base, "/") {
      base = fmt.Sprintf("/%s", base)
    }
    if ! strings.HasPrefix(api, "/") {
      api = fmt.Sprintf("/%s", base)
    }
  }
  return fmt.Sprintf("%s%s", base, api)
}

// Setup http routes
func Handlers() *mux.Router {
  router := mux.NewRouter()
  fmt.Println(getPath("hello"))
  router.HandleFunc(getPath("/hello"), HelloHandler)
  return router
}

// Start server according to parameters in configFile
func Server(configFile string) {
  ParseConfig(configFile)
  addr := fmt.Sprintf(":%d", Config.Port)
  server := &http.Server{Addr: addr}
  http.Handle(getPath("/"), Handlers())
  err := server.ListenAndServe()
  if err != nil {
    log.Fatalf("Unable to start server, %v\n", err)
  }
}
