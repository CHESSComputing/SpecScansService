package main

import (
	"encoding/json"
	"log"
	"net/http"
)

// Helper for handling errors
func HTTPError(label, msg string, w http.ResponseWriter) {
	log.Println(label, msg)
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(msg))
	return
}

func HelloHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("hello from HelloHandler")
	w.Write([]byte("hello"))
	return
}

// Handler for adding a new record to the database
func AddHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		HTTPError("ERROR", "requests to /add must use POST", w)
		return
	}

	defer r.Body.Close()
	var record Record
	err := json.NewDecoder(r.Body).Decode(&record)
	if err != nil {
		HTTPError("ERROR", "Cannot decode body of request as JSON", w)
		return
	}

	log.Printf("Add record: %v\n", record)
	w.WriteHeader(http.StatusOK)
	return
}

// Handler for editing a record already in the database
func EditHandler(w http.ResponseWriter, r *http.Request) {
	id := r.FormValue("id")
  if id == "" {
    HTTPError("ERROR", "No record id in form", w)
    return
  }
	log.Printf("Editing record %s\n", id)
	if r.Method == "GET" {
		// Respond with html form to edit the record
		log.Printf("Construct record-editing HTML")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("<p>record-editing form goes here</p>"))
		return
	}
	if r.Method == "POST" {
		// Update the record in the db
		log.Printf("Update the record in the database")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Record updated"))
		return
	}
}

// Handler for querying the database for records
func SearchHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		// Respond with html form to construct "advanced" searches
		log.Printf("Construct advanced search HTML")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("<p>search form goes here</p>"))
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
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("<p>table of matching records goes here</p>"))
			return
		}
	}
}
