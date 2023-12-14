package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"gopkg.in/jcmturner/gokrb5.v7/client"
	"gopkg.in/jcmturner/gokrb5.v7/config"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Helper for handling errors
func HTTPError(label, msg string, w http.ResponseWriter) {
	log.Println(label, msg)
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(msg))
	return
}

// Helper for getting username from a request
func getUsername(r *http.Request) (string, error) {
	if Config.TestMode {
		return "test", nil
	}
	cookie, err := r.Cookie("auth-session")
	if err != nil {
		return "", err
	}
	// Cookie added by KAuthHandler has a "Value" that looks like: username-authenticated
	s := cookie.Value
	splitIndex := strings.LastIndex(s, "-")
	if splitIndex == -1 {
		return "", errors.New("Unable to get username from auth-session")
	}
	return s[:splitIndex], nil
}

func HelloHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("hello from HelloHandler")
	w.Write([]byte("hello"))
	return
}

// Handler for all requests: log some general info
func BaseHandler(w http.ResponseWriter, r *http.Request) {
	user, err := getUsername(r)
	if err != nil {
		LoginHandler(w, r)
	}
	log.Printf("User %s path %v\n", user, r.URL.Path)
	w.Write([]byte(htmlTop + "hello" + htmlBottom))
}

// Handler for providing kerberos authentication
func KAuthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	// Get username & password
	err := r.ParseForm()
	if err != nil {
		HTTPError("ERROR", "Cannot parse HTTP form", w)
		return
	}
	username := r.FormValue("username")
	password := r.FormValue("password")
	if username == "" || password == "" {
		HTTPError("ERROR", "Missing username or password", w)
		return
	}

	if Config.TestMode {
		w.WriteHeader(http.StatusContinue)
	} else {
		// Load KRB5 configuration
		krb5conf, err := config.Load(Config.Krb5Conf)
		if err != nil {
			log.Printf("Cannot load KRB5 configuration from %s", Config.Krb5Conf)
			HTTPError("ERROR", "Cannot perform KRB5 authentication", w)
			return
		}

		// Perform login
		client := client.NewClientWithPassword(username, Config.Realm, password, krb5conf, client.DisablePAFXFAST(true))
		err = client.Login()
		if err != nil {
			HTTPError("ERROR", "Cannot login with username/password provided", w)
			return
		}

		// Set cookie with client credentials
		expires := time.Now().Add(24 * time.Hour)
		value := fmt.Sprintf("%s-%v", client.Credentials.UserName(), client.Credentials.Authenticated())
		cookie := http.Cookie{Name: "auth-session", Value: value, Expires: expires}
		http.SetCookie(w, &cookie)
		w.WriteHeader(http.StatusCreated)
	}
	w.Header().Set("Location", "/")
}

// Handler for login
func LoginHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	tmplData := MakeTmplData()
	htmlLogin := FormatTemplate(Config.TemplateDir, "login.tmpl", tmplData)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(htmlTop + htmlLogin + htmlBottom))
}

// Handler for adding a new record to the database
func AddHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	defer r.Body.Close()
	var record Record
	err := json.NewDecoder(r.Body).Decode(&record)
	if err != nil {
		HTTPError("ERROR", "Cannot decode body of request as JSON", w)
		return
	}

	// Connect to MongoDb
	log.Printf("Connecting to %s", Config.MongodbUri)
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(Config.MongodbUri))
	if err != nil {
		HTTPError("ERROR", "Cannot connect to database", w)
		return
	}
	defer func() {
		if err := client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()
	// Get the Mongodb collection of interest
	coll := client.Database(Config.MongodbName).Collection(Config.MongodbCollection)
	result, err := coll.InsertOne(context.TODO(), &record)
	if err != nil {
		HTTPError("ERROR", "Cannot insert record", w)
		return
	}
	log.Printf("Added record: %v (ID: %v)", record, result.InsertedID)
	w.WriteHeader(http.StatusOK)
	return
}

// Handler for editing a record already in the database
func EditHandler(w http.ResponseWriter, r *http.Request) {
	// Get ID of record to edit
	id := r.FormValue("id")
	if id == "" {
		HTTPError("ERROR", "No record id in form", w)
		return
	}
	// Ensure requesting user is allowed to edit this record
	username, err := getUsername(r)
	if err != nil {
		HTTPError("ERROR", "Cannot determine username", w)
		return
	}
	// Check with BeamPass: user must associated with the BTR of this record
	log.Printf("User %s attempting to edit record %s\n", username, id)
	if r.Method == "GET" {
		// Respond with html form to edit the record
		log.Printf("Construct record-editing HTML")
		tmplData := MakeTmplData()
		tmplData["Id"] = id
		tmplData["Form"] = "Form inputs go here"
		htmlForm := FormatTemplate(Config.TemplateDir, "editform.tmpl", tmplData)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(htmlTop + htmlForm + htmlBottom))
		return
	}
	if r.Method == "POST" {
		// Update the record in the db
		log.Printf("Update record %s in the database", id)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(fmt.Sprintf("Record %s updated", id)))
		return
	}
}

// Handler for querying the database for records
func SearchHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		// Respond with html form to construct "advanced" searches
		log.Printf("Construct advanced search HTML")
		tmplData := MakeTmplData()
		htmlForm := FormatTemplate(Config.TemplateDir, "searchform.tmpl", tmplData)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(htmlTop + htmlForm + htmlBottom))
		return
	}
	if r.Method == "POST" {
		// Perform search using parameters from the form;
		// get slice of matching Records
		var selectors struct{}
		err := json.NewDecoder(r.Body).Decode(&selectors)
		if err != nil {
			HTTPError("ERROR", "Cannot decode body of request as JSON", w)
		}

		log.Printf("Search database with selectors: %v\n", selectors)
		var records []Record
		log.Printf("Matching records: %v\n", records)

		if client := r.FormValue("client"); client == "cli" {
			// Respond with matching records as JSON data
			data, err := json.Marshal(records)
			if err != nil {
				HTTPError("ERROR", "Unable to marshal matching records as JSON", w)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write(data)
			return
		} else {
			// Respond with matching records as HTML table
			tmplData := MakeTmplData()
			htmlTable := FormatTemplate(Config.TemplateDir, "searchresults.tmpl", tmplData)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(htmlTop + htmlTable + htmlBottom))
			return
		}
	}
}
