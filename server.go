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
	mongo "github.com/CHESSComputing/golib/mongo"
	ql "github.com/CHESSComputing/golib/ql"
)

var _httpReadRequest *services.HttpRequest
var Verbose int
var QLM ql.QLManager

// helper function to setup our router
func setupRouter() *gin.Engine {
	routes := []server.Route{
		server.Route{Method: "POST", Path: "/add", Handler: AddHandler, Authorized: true, Scope: "write"},
		server.Route{Method: "POST", Path: "/edit", Handler: EditHandler, Authorized: true, Scope: "write"},
		server.Route{Method: "POST", Path: "/search", Handler: SearchHandler, Authorized: true},
	}
	r := server.Router(routes, nil, "static", srvConfig.Config.SpecScans.WebServer) // FIX temporary config
	return r
}

// Server defines our HTTP server
func Server() {
	Verbose = srvConfig.Config.SpecScans.WebServer.Verbose // FIX temporary config
	_httpReadRequest = services.NewHttpRequest("read", Verbose)

	// Setup mongodb connection
	mongo.InitMongoDB(srvConfig.Config.SpecScans.MongoDB.DBUri)
	// Setup sqlite3 db connection
	InitMotorsDb()
	defer MotorsDb.db.Close()

	// Initialize map of component databased & query keys belonging to each one
	// TODO: add field for QLMFile in golib/config.SpecScans, then use srvConfig.Config.SpecScans.QLMFile as the arg to QLM.Init
	// initialize QL map
	QLM.Init(srvConfig.Config.QL.ServiceMapFile)

	// setup web router and start the service
	r := setupRouter()
	webServer := srvConfig.Config.SpecScans.WebServer // FIX temporary config
	server.StartServer(r, webServer)
}
