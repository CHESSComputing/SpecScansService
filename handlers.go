package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	mapstructure "github.com/mitchellh/mapstructure"
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
	var records []UserRecord
	err = json.Unmarshal(body, &records)
	if err != nil {
		// Fallback: try to unmarshal body as single record
		var record UserRecord
		err = json.Unmarshal(body, &record)
		if err != nil {
			log.Printf("Unmarshal error: %v", err)
			resp := services.Response("SpecScans", http.StatusInternalServerError, services.UnmarshalError, err)
			c.JSON(http.StatusInternalServerError, resp)
			return
		}
		records = []UserRecord{record}
	}

	if Verbose > 0 {
		log.Printf("AddHandler received request %+v", records)
	}
	rec_ch := make(chan map[string]any)
	err_ch := make(chan error)
	defer close(rec_ch)
	defer close(err_ch)
	for _, record := range records {
		go addRecord(record, rec_ch, err_ch)
	}
	var result_records []map[string]any
	var result_err string
	for i := 0; i < len(records); i++ {
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

	matching_records := *new([]UserRecord)

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
	log.Printf("### query: %+v", query)
	queries, err := getServiceQueriesByDBType(QLM, "SpecScans", query)
	if err != nil {
		resp := services.Response("SpecScans", http.StatusInternalServerError, services.ParseError, err)
		c.JSON(http.StatusInternalServerError, resp)
		return
	}
	log.Printf("queries %+v", queries)

	if queries["mongo"] == nil {
		if queries["sql"] == nil {
			// queries["mongo"] == nil && queries["sql"] == nil
			// User query is empty -- match _all_ records
			mongo_records, err := getMongoRecords(bson.M{}, idx, limit)
			if err != nil {
				resp := services.Response("SpecScans", http.StatusInternalServerError, services.QueryError, err)
				c.JSON(http.StatusInternalServerError, resp)
				return
			}
			matching_records, err = CompleteMongoRecords(mongo_records...)
			if err != nil {
				resp := services.Response("SpecScans", http.StatusInternalServerError, services.QueryError, err)
				c.JSON(http.StatusInternalServerError, resp)
				return
			}
		} else {
			// queries["mongo"] == nil && queries["sql"] != nil
			// Search for matching records by motor positions only, then complete all
			// the matching motor records with their mongodb portion
			motor_records, err := getMotorRecords(queries["sql"])
			if err != nil {
				resp := services.Response("SpecScans", http.StatusInternalServerError, services.QueryError, err)
				c.JSON(http.StatusInternalServerError, resp)
				return
			}
			matching_records, err = CompleteMotorRecords(motor_records...)
			if err != nil {
				resp := services.Response("SpecScans", http.StatusInternalServerError, services.QueryError, err)
				c.JSON(http.StatusInternalServerError, resp)
				return
			}
		}
	} else {
		mongo_records, err := getMongoRecords(queries["mongo"], idx, limit)
		if err != nil {
			resp := services.Response("SpecScans", http.StatusInternalServerError, services.QueryError, err)
			c.JSON(http.StatusInternalServerError, resp)
			return
		}
		if queries["sql"] == nil {
			// queries["mongo"] != nil && queries["sql"] == nil
			// Search for matching records in the mongodb only, then complete all
			// matching mongo records with their motors component
			matching_records, err = CompleteMongoRecords(mongo_records...)
			if err != nil {
				resp := services.Response("SpecScans", http.StatusInternalServerError, services.QueryError, err)
				c.JSON(http.StatusInternalServerError, resp)
				return
			}
		} else {
			// queries["mongo"] != nil && queries["sql"] != nil
			// Search both dbs separately, then return the _intersection_ of the two
			// matching sets (NB: doesn't allow conditional filtering on fields in
			// separate dbs!).
			motor_records, err := getMotorRecords(queries["sql"])
			if err != nil {
				resp := services.Response("SpecScans", http.StatusInternalServerError, services.QueryError, err)
				c.JSON(http.StatusInternalServerError, resp)
				return
			}
			matching_records = getIntersectionRecords(mongo_records, motor_records)
		}
	}
	var map_records []map[string]any
	err = mapstructure.Decode(matching_records, &map_records)
	if err != nil {
		resp := services.Response("SpecScans", http.StatusInternalServerError, services.ParseError, err)
		c.JSON(http.StatusInternalServerError, resp)
		return
	}
	response := services.ServiceResponse{
		HttpCode: http.StatusOK,
		SrvCode:  services.OK,
		Service:  "SpecScans",
		Results: services.ServiceResults{
			NRecords: len(map_records),
			Records:  map_records,
		},
	}
	c.JSON(http.StatusOK, response)
	return
}

// Helper function to add a single record to the database(s)
// (to be called as a goroutine)
func addRecord(record UserRecord, rec_ch chan map[string]any, err_ch chan error) {
	var record_map map[string]any
	err := mapstructure.Decode(record, &record_map)
	if err != nil {
		err_ch <- err
		return
	}

	err = lexicon.ValidateRecord(record_map)
	if err != nil {
		err_ch <- err
		return
	}

	err = Schema.Validate(record_map)
	if err != nil {
		err_ch <- err
		return
	}

	// Decompose the user-submitted record into the portions will be submitted to
	// the two separate dbs.
	mongo_record, motor_record := DecomposeRecord(record)

	// Insert the motor mnes & positions record
	// (do this first since we can easily check the uniqueness of the new record's
	//  scan ID with the SQL db)
	_, err = InsertMotors(motor_record)
	if err != nil {
		err_ch <- err
		return
	}

	// If submitting the motor record was successful, submit the other portion of
	// the record to mongodb.
	var mongo_record_map map[string]any
	err = mapstructure.Decode(mongo_record, &mongo_record_map)
	if err != nil {
		err_ch <- err
		return
	}
	mongo.Insert(srvConfig.Config.SpecScans.MongoDB.DBName, srvConfig.Config.SpecScans.MongoDB.DBColl, []map[string]any{mongo_record_map})

	// Send SID of new record
	result_record := map[string]any{"sid": mongo_record.ScanId}
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
	for key, val := range specscanquery {
		for _, rec := range q.Records {
			if rec.Service == servicename && (key == rec.Key || strings.HasPrefix(key, fmt.Sprintf("%s.", rec.Key))) {
				if _, ok := dbqueries[rec.DBType]; ok {
					dbqueries[rec.DBType][key] = val
				} else {
					dbqueries[rec.DBType] = bson.M{key: val}
				}
			}
		}
	}
	return dbqueries, nil
}

// Get matching records from the mongodb only
func getMongoRecords(query bson.M, idx int, limit int) ([]MongoRecord, error) {
	var mongo_records []MongoRecord
	nrecords := mongo.Count(srvConfig.Config.SpecScans.MongoDB.DBName, srvConfig.Config.SpecScans.MongoDB.DBColl, query)
	records := mongo.Get(srvConfig.Config.SpecScans.MongoDB.DBName, srvConfig.Config.SpecScans.MongoDB.DBColl, query, idx, limit)
	if Verbose > 0 {
		log.Printf("spec %v nrecords %d return idx=%d limit=%d", query, nrecords, idx, limit)
	}
	for _, record := range records {
		var mongo_record MongoRecord
		err := mapstructure.Decode(record, &mongo_record)
		if err != nil {
			return mongo_records, err
		}
		mongo_records = append(mongo_records, mongo_record)
	}
	return mongo_records, nil
}

func getMotorRecords(query bson.M) ([]MotorRecord, error) {
	motor_records := QueryMotorsDb(query)
	if Verbose > 0 {
		log.Printf("query %v found %d records\n", query, len(motor_records))
	}
	return motor_records, nil
}
