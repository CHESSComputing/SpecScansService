package main

// server module
//
// Copyright (c) 2023 - Valentin Kuznetsov <vkuznet@gmail.com>
//
import (
	server "github.com/CHESSComputing/golib/server"
	services "github.com/CHESSComputing/golib/services"
	"github.com/gin-gonic/gin"

	srvConfig "github.com/CHESSComputing/golib/config"
)

var _httpReadRequest *services.HttpRequest
var Verbose int

// helper function to setup our router
func setupRouter() *gin.Engine {
	routes := []server.Route{
		server.Route{Method: "POST", Path: "/add", Handler: AddHandler, Authorized: true, Scope: "write"},
		server.Route{Method: "POST", Path: "/edit", Handler: EditHandler, Authorized: true, Scope: "write"},
		server.Route{Method: "POST", Path: "/search", Handler: SearchHandler, Authorized: true},
	}
	r := server.Router(routes, nil, "static", srvConfig.Config.MetaData.WebServer) // FIX temporary config
	return r
}

// Server defines our HTTP server
func Server() {
	Verbose = srvConfig.Config.MetaData.WebServer.Verbose // FIX temporary config
	_httpReadRequest = services.NewHttpRequest("read", Verbose)

	// setup web router and start the service
	r := setupRouter()
	webServer := srvConfig.Config.MetaData.WebServer // FIX temporary config
	server.StartServer(r, webServer)
}
