package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	bson "go.mongodb.org/mongo-driver/bson"

	auth "github.com/CHESSComputing/golib/authz"
	srvConfig "github.com/CHESSComputing/golib/config"
	lexicon "github.com/CHESSComputing/golib/lexicon"
	mongo "github.com/CHESSComputing/golib/mongo"
	ql "github.com/CHESSComputing/golib/ql"
	services "github.com/CHESSComputing/golib/services"
)

// Handler for adding new records to the database
func AddHandler(c *gin.Context) {
	// Get single record OR multiple records to submit
	defer c.Request.Body.Close()
	body, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		log.Printf("ReadAll error: %v", err)
		resp := services.Response("SpecScans", http.StatusInternalServerError, services.ReaderError, err)
		c.JSON(http.StatusInternalServerError, resp)
		return
	}

	// Try to unmarshal body as multiple records
	var record_maps []map[string]any
	err = json.Unmarshal(body, &record_maps)
	if err != nil {
		// Fallback: try to unmarshal body as single record
		var record_map map[string]any
		err = json.Unmarshal(body, &record_map)
		if err != nil {
			log.Printf("Unmarshal error: %v", err)
			resp := services.Response("SpecScans", http.StatusInternalServerError, services.UnmarshalError, err)
			c.JSON(http.StatusInternalServerError, resp)
			return
		}
		record_maps = []map[string]any{record_map}
	}

	if Verbose > 0 {
		log.Printf("AddHandler received request %+v", record_maps)
	}
	rec_ch := make(chan map[string]any)
	err_ch := make(chan error)
	defer close(rec_ch)
	defer close(err_ch)
	for _, record_map := range record_maps {
		go addRecord(record_map, rec_ch, err_ch)
	}
	var result_records []map[string]any
	var result_err string
	for i := 0; i < len(record_maps); i++ {
		select {
		case new_record := <-rec_ch:
			result_records = append(result_records, new_record)
			log.Printf("New record: %+v", new_record)
		case add_err := <-err_ch:
			result_err = fmt.Sprintf("%s; %s", result_err, add_err)
			log.Printf("Error adding record: %s", add_err)
		}
	}
	response := services.ServiceResponse{
		HttpCode: http.StatusOK,
		SrvCode:  services.OK,
		Service:  "SpecScans",
		Error:    result_err,
		Results: services.ServiceResults{
			NRecords: len(result_records),
			Records:  result_records,
		},
	}
	c.JSON(http.StatusOK, response)
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
	log.Printf("service request: %+v", query_request)

	// Get all attributes we need for querying the mongodb
	query := query_request.ServiceQuery.Query
	idx := query_request.ServiceQuery.Idx
	limit := query_request.ServiceQuery.Limit

	// Get query string as map of values
	queries, err := getServiceQueriesByDBType(QLM, "SpecScans", query)
	if err != nil {
		resp := services.Response("SpecScans", http.StatusInternalServerError, services.ParseError, err)
		c.JSON(http.StatusInternalServerError, resp)
		return
	}
	log.Printf("Mongo query: %v", queries["mongo"])
	log.Printf("SQL query: %v", queries["sql"])

	// Query the mongodb
	nrecords := mongo.Count(srvConfig.Config.SpecScans.MongoDB.DBName, srvConfig.Config.SpecScans.MongoDB.DBColl, queries["mongo"])
	records := mongo.Get(srvConfig.Config.SpecScans.MongoDB.DBName, srvConfig.Config.SpecScans.MongoDB.DBColl, queries["mongo"], idx, limit)
	if Verbose > 0 {
		log.Printf("spec %v nrecords %d return idx=%d limit=%d", queries["mongo"], nrecords, idx, limit)
	}

	// Query the SQL db of motor positions
	if queries["sql"] != nil {
		motor_records := QueryMotorsDb(queries["sql"]["motors"])
    
		if queries["mongo"] != nil {
			// Aggregate intersection of results from both dbs
			var intersection_records []map[string]any
			for _, record := range records {
				sid := uint64(record["sid"].(int64))
				for _, motor_record := range motor_records {
					if motor_record.ScanId == sid {
						intersection_records = append(intersection_records, CompleteRecord(record, motor_record))
						break
					}
				}
			}
			records = intersection_records
		} else {
			// Complete the matching records with mongodb documents
			var sids []uint64
			for _, motor_record := range motor_records {
				sids = append(sids, motor_record.ScanId)
			}
			mongo_query := bson.M{"sid": bson.M{"$in": sids}}
			mongo_records := mongo.Get(srvConfig.Config.SpecScans.MongoDB.DBName, srvConfig.Config.SpecScans.MongoDB.DBColl, mongo_query, idx, limit)
			for _, mongo_record := range mongo_records {
				for _, motor_record := range motor_records {
					if motor_record.ScanId == uint64(mongo_record["sid"].(int64)) {
						records = append(records, CompleteRecord(mongo_record, motor_record))
						break
					}
				}
			}
		}
	} else {
		// Complete the matching records with motor positions
		log.Println("Completing mongo records with motor records")
		var sids []uint64
		for _, record := range records {
			sids = append(sids, uint64(record["sid"].(int64)))
		}
		motor_records, err := GetMotorRecords(sids...)
		if err != nil {
			resp := services.Response("SpecScans", http.StatusInternalServerError, services.DatabaseError, err)
			c.JSON(http.StatusInternalServerError, resp)
			return
		}
		for i, record := range records {
			for _, motor_record := range motor_records {
				if motor_record.ScanId == uint64(record["sid"].(int64)) {
					records[i] = CompleteRecord(record, motor_record)
				}
			}
		}

	}

	c.JSON(http.StatusOK, records)
	return
}

// Helper function to add a single record to the database(s)
// (to be called as a goroutine)
func addRecord(record map[string]any, rec_ch chan map[string]any, err_ch chan error) {
	err := lexicon.ValidateRecord(record)
	if err != nil {
		err_ch <- err
		return
	}

	err = Schema.Validate(record)
	if err != nil {
		err_ch <- err
		return
	}

	// Decompose the user-submitted record into the portions will be submitted to
	// the two separate dbs.
	mongo_record, motor_record := DecomposeRecord(record)

	// Submit one portion of the record to mongodb...
	mongo.Insert(srvConfig.Config.SpecScans.MongoDB.DBName, srvConfig.Config.SpecScans.MongoDB.DBColl, []map[string]any{mongo_record})

	// Now insert the motor mnes & positions record
	sql_id, err := InsertMotors(motor_record)
	if err != nil {
		err_ch <- err
		return
	}

	// Send SID of new record
	result_record := map[string]any{"sid": sql_id}
	rec_ch <- result_record
}

// Returns map of queries for a single service sorted by the query
// keys' DBType
func getServiceQueriesByDBType(q ql.QLManager, servicename string, query string) (map[string]bson.M, error) {
	dbqueries := make(map[string]bson.M)
	queries, err := q.ServiceQueries(query)
	if err != nil {
		return dbqueries, err
	}
	specscanquery := queries[servicename]
	for key, val := range(specscanquery) {
		for _, rec := range(q.Records) {
			if rec.Key == key && rec.Service == servicename {
				if dbquery, ok := dbqueries[rec.DBType]; ok {
					dbquery[key] = val
					dbqueries[rec.DBType] = dbquery
				} else {
					dbqueries[rec.DBType] = bson.M{key: val}
				}
			}
		}
	}
	return dbqueries, nil
}
