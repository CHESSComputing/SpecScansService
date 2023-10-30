package main

import (
  "fmt"
  "net/http"
)

func HelloHandler(w http.ResponseWriter, r *http.Request) {
  fmt.Println("hello from HelloHandler")
  w.Write([]byte("hello"))
}
