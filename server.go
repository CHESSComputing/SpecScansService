package main

// server module
//
// Copyright (c) 2023 - Valentin Kuznetsov <vkuznet@gmail.com>
//
import (
	"log"

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
		{Method: "POST", Path: "/add", Handler: AddHandler, Authorized: true, Scope: "write"},
		{Method: "PUT", Path: "/edit", Handler: EditHandler, Authorized: true, Scope: "write"},
		{Method: "POST", Path: "/search", Handler: SearchHandler, Authorized: true},
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

	// Setup motorsdb connection
	InitMotorsDb()

	// local SpecScans schema
	InitSchemaManager()

	// initialize QLM
	err := QLM.Init(srvConfig.Config.QL.ServiceMapFile)
	if err != nil {
		log.Fatal(err)
	}

	// setup web router and start the service
	r := setupRouter()
	webServer := srvConfig.Config.SpecScans.WebServer // FIX temporary config
	server.StartServer(r, webServer)
}
