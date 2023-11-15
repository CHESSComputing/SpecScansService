package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
)

type Configuration struct {
	Port        int    `json:"port"`
	Base        string `json:"string"`
	TemplateDir string `json:"templatedir"`
	Krb5Conf    string `json:"krb5conf"`
	Realm       string `json:"realm"`
	TestMode    bool   `json:"testmode"`
}

var Config Configuration

var htmlTop, htmlBottom string

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
		if !strings.HasPrefix(base, "/") {
			base = fmt.Sprintf("/%s", base)
		}
		if !strings.HasPrefix(api, "/") {
			api = fmt.Sprintf("/%s", base)
		}
	}
	return fmt.Sprintf("%s%s", base, api)
}

// Setup http routes
func Handlers() *mux.Router {
	router := mux.NewRouter()
	router.HandleFunc(getPath("/hello"), HelloHandler)
	router.HandleFunc(getPath("/login"), LoginHandler)
	router.HandleFunc(getPath("/auth"), KAuthHandler)
	router.HandleFunc(getPath("/add"), AddHandler)
	router.HandleFunc(getPath("/edit"), EditHandler)
	router.HandleFunc(getPath("/search"), SearchHandler)
	router.HandleFunc(getPath("/"), BaseHandler)
	return router
}

// Start server according to parameters in configFile
func Server(configFile string) {
	// Load configuration to Config
	ParseConfig(configFile)

	// Setup HTML templates
	tmplData := MakeTmplData()
	htmlTop = FormatTemplate(Config.TemplateDir, "top.tmpl", tmplData)
	htmlBottom = FormatTemplate(Config.TemplateDir, "bottom.tmpl", tmplData)

	// Start server
	addr := fmt.Sprintf(":%d", Config.Port)
	server := &http.Server{Addr: addr}
	http.Handle(getPath("/"), Handlers())
	err := server.ListenAndServe()
	if err != nil {
		log.Fatalf("Unable to start server, %v\n", err)
	}
}
