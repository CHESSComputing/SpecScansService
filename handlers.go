package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"

	structtomap "github.com/Klathmon/StructToMap"

	auth "github.com/CHESSComputing/golib/authz"
	srvConfig "github.com/CHESSComputing/golib/config"
	mongo "github.com/CHESSComputing/golib/mongo"
	services "github.com/CHESSComputing/golib/services"
	utils "github.com/CHESSComputing/golib/utils"
)

// Helper for handling errors
func HTTPError(label, msg string, w http.ResponseWriter) {
	log.Println(label, msg)
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(msg))
	return
}

// Handler for adding a new record to the database
func AddHandler(c *gin.Context) {
	var record Record
	if err := c.Bind(&record); err != nil {
		resp := services.Response("SpecScans", http.StatusInternalServerError, services.BindError, err)
		c.JSON(http.StatusInternalServerError, resp)
		return
	}
	log.Printf("New record: %v", record)
	record_map, err := structtomap.Convert(record)
	if err != nil {
		resp := services.Response("SpecScans", http.StatusInternalServerError, services.ParseError, err)
		c.JSON(http.StatusInternalServerError, resp)
		return
	}

	// Get the dataset ID and add it to the records to be submitted
	attrs := srvConfig.Config.DID.Attributes
	sep := srvConfig.Config.DID.Separator
	div := srvConfig.Config.DID.Divider
	did := utils.CreateDID(record_map, attrs, sep, div)
	record_map["DatasetID"] = did
	log.Printf("New record DID: %s", did)
	motor_record := MotorRecord{
		DatasetId:      did,
		MotorMnes:      record.MotorMnes,
		MotorPositions: record.MotorPositions}
	// Peel off motor mnemonics & positions -- these are submitted to an rdb, not
	// the mongodb.
	record_map["MotorMnes"] = nil
	record_map["MotorPositions"] = nil

	// Sumnit one portion of the record to mongodb...
	mongo_records := []map[string]any{record_map} // FIXME record is not a map[string]any...
	mongo.Insert(srvConfig.Config.SpecScans.MongoDB.DBName, srvConfig.Config.SpecScans.MongoDB.DBColl, mongo_records)

	// Now insert the motor mnes & positions record
	sql_id, err := InsertMotors(motor_record)
	if err != nil {
		resp := services.Response("SpecScans", http.StatusInternalServerError, services.InsertError, err)
		c.JSON(http.StatusInternalServerError, resp)
		return
	}
	log.Printf("Added record to SQL db: %v (ID: %v)\n", motor_record, sql_id)

	c.String(http.StatusOK, fmt.Sprintf("New record ID: %s\n", did))
	return
}

// Handler for editing a record already in the database
func EditHandler(c *gin.Context) {
	// Get ID of record to edit
	id := c.Param("id")
	if id == "" {
		err := errors.New("ID of record to edit not found")
		resp := services.Response("SpecScans", http.StatusInternalServerError, services.FormDataError, err)
		c.JSON(http.StatusInternalServerError, resp)
		return
	}
	// Ensure requesting user is allowed to edit this record
	username, err := auth.UserCredentials(c.Request)
	if err != nil {
		resp := services.Response("SpecScans", http.StatusInternalServerError, services.CredentialsError, err)
		c.JSON(http.StatusInternalServerError, resp)
		return
	}
	// Check with BeamPass: user must associated with the BTR of this record
	log.Printf("User %s attempting to edit record %s\n", username, id)
	log.Printf("Placeholder: Update record %s in the database", id)
	c.String(http.StatusOK, fmt.Sprintf("Record %s updated", id))
	return
}

// Handler for querying the databases for records
func SearchHandler(c *gin.Context) {

	// Parse database query from request
	var query_request services.ServiceRequest
	if err := c.Bind(&query_request); err != nil {
		resp := services.Response("SpecScans", http.StatusInternalServerError, services.ParseError, err)
		c.JSON(http.StatusInternalServerError, resp)
		return
	}
	// Get all attributes we need for querying the mongodb
	query := query_request.ServiceQuery.Query
	idx := query_request.ServiceQuery.Idx
	limit := query_request.ServiceQuery.Limit

	// Get query string as map of values
	queries, err := QLM.ServiceQueries(query)
	if err != nil {
		resp := services.Response("SpecScans", http.StatusInternalServerError, services.ParseError, err)
		c.JSON(http.StatusInternalServerError, resp)
		return
	}
	log.Printf("Mongo query: %v", queries["Mongo"])
	log.Printf("SQL query: %v", queries["SQL"])

	// Query the mongodb
	var records []map[string]any
	nrecords := 0
	if queries["Mongo"] != nil {
		nrecords = mongo.Count(srvConfig.Config.SpecScans.MongoDB.DBName, srvConfig.Config.SpecScans.MongoDB.DBColl, queries["Mongo"])
		records = mongo.Get(srvConfig.Config.SpecScans.MongoDB.DBName, srvConfig.Config.SpecScans.MongoDB.DBColl, queries["Mongo"], idx, limit)
	}
	if Verbose > 0 {
		log.Printf("spec %v nrecords %d return idx=%d limit=%d", queries["Mongo"], nrecords, idx, limit)
	}

	if queries["SQL"] != nil {
		mne := queries["SQL"]["motor"].(string)
		pos := queries["SQL"]["position"].(float64)
		motor_records := QueryMotorPosition(mne, pos)
		log.Printf("Matching motor records: %v", motor_records)
	}

	// TODO: Aggregate results from both dbs

	c.JSON(http.StatusOK, records)
	return
}
