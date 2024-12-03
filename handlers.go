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
	var httpcode, srvcode int
	if result_err == "" {
		httpcode = http.StatusOK
		srvcode = services.OK
	} else {
		if len(result_records) == 0 {
			httpcode = http.StatusUnprocessableEntity
		} else {
			httpcode = http.StatusMultiStatus
		}
		srvcode = services.TransactionError
	}
	response := services.ServiceResponse{
		HttpCode: httpcode,
		SrvCode:  srvcode,
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
	// Get single record OR multiple records to edit
	defer c.Request.Body.Close()
	body, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		log.Printf("ReadAll error: %v", err)
		resp := services.Response("SpecScans", http.StatusInternalServerError, services.ReaderError, err)
		c.JSON(http.StatusInternalServerError, resp)
		return
	}

	// Try to unmarshal body as multiple record edits
	var edits []map[string]any
	err = json.Unmarshal(body, &edits)
	if err != nil {
		// Fallback: try to unmarshal body as single record edit
		var edit map[string]any
		err = json.Unmarshal(body, &edit)
		if err != nil {
			log.Printf("Unmarshal error: %v", err)
			resp := services.Response("SpecScans", http.StatusInternalServerError, services.UnmarshalError, err)
			c.JSON(http.StatusInternalServerError, resp)
			return
		}
		edits = []map[string]any{edit}
	}

	if Verbose > 0 {
		log.Printf("EditHandler received request %+v", edits)
	}

	rec_ch := make(chan map[string]any)
	err_ch := make(chan error)
	defer close(rec_ch)
	defer close(err_ch)
	for _, edit := range edits {
		go editRecord(edit, rec_ch, err_ch)
	}
	var result_records []map[string]any
	var result_err string
	for i := 0; i < len(edits); i++ {
		select {
		case new_record := <-rec_ch:
			result_records = append(result_records, new_record)
			log.Printf("Edited record: %+v", new_record)
		case edit_err := <-err_ch:
			result_err = fmt.Sprintf("%s; %s", result_err, edit_err)
			log.Printf("Error editing record: %s", edit_err)
		}
	}
	var httpcode, srvcode int
	if result_err == "" {
		httpcode = http.StatusOK
		srvcode = services.OK
	} else {
		if len(result_records) == 0 {
			httpcode = http.StatusUnprocessableEntity
		} else {
			httpcode = http.StatusMultiStatus
		}
		srvcode = services.TransactionError
	}
	response := services.ServiceResponse{
		HttpCode: httpcode,
		SrvCode:  srvcode,
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

	spec, err := ql.ParseQuery(query)
	if Verbose > 0 {
		log.Printf("search query='%s' spec=%+v", query, spec)
	}
	if err != nil {
		rec := services.Response("SpecScans", http.StatusInternalServerError, services.ParseError, err)
		c.JSON(http.StatusInternalServerError, rec)
		return
	}
	if len(spec) == 0 &&
		strings.Contains(query, srvConfig.Config.DID.Separator) &&
		strings.Contains(query, srvConfig.Config.DID.Divider) {
		// User's query string did not represent a mapping, but it could be a DID.
		query = fmt.Sprintf("{\"did\": \"%s\"}", query)
	}

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
			mongo_records, err := getMongoRecords(map[string]any{}, idx, limit)
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
		HttpCode:     http.StatusOK,
		SrvCode:      services.OK,
		Service:      "SpecScans",
		ServiceQuery: query_request.ServiceQuery,
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
	_, err := validateRecord(record)
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
	_, err = InsertMotors(motor_record, MotorsDb)
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
	mongo.Insert(
		srvConfig.Config.SpecScans.MongoDB.DBName,
		srvConfig.Config.SpecScans.MongoDB.DBColl,
		[]map[string]any{mongo_record_map})

	// Send SID of new record
	result_record := map[string]any{"sid": mongo_record.ScanId}
	rec_ch <- result_record
}

// Helper function to edit a single record in the database(s)
// (to be called as a goroutine)
func editRecord(edit map[string]any, rec_ch chan map[string]any, err_ch chan error) {
	// Get unedited version of the record to edit as map[string]any
	// (look it up by start_time or spec_file & scan_number, whichever is available)
	query := map[string]any{}
	sid, ok := edit["start_time"]
	if !ok {
		spec_file, ok := edit["spec_file"]
		if !ok {
			err_ch <- errors.New("Edit must contain start_time or spec_file and scan_number to identify the record to edit")
			return
		}
		scan_number, ok := edit["scan_number"]
		if !ok {
			err_ch <- errors.New("Edit must contain start_time or spec_file and scan_number to identify the record to edit")
			return
		}
		query["spec_file"] = spec_file
		query["scan_number"] = scan_number
	} else {
		query["start_time"] = sid
	}
	original_records, err := getMongoRecords(query, 0, 0)
	if len(original_records) != 1 {
		err_ch <- errors.New(fmt.Sprintf("Edit request matched %d existing records. Must match exactly 1.", len(original_records)))
		return
	}
	var edited_record map[string]any
	err = mapstructure.Decode(original_records[0], &edited_record)
	if err != nil {
		err_ch <- err
		return
	}
	for k, v := range edit {
		edited_record[k] = v
	}
	_, err = validateRecord(edited_record)
	if err != nil {
		err_ch <- err
		return
	}
	// Update the record with the edited parameters
	update_spec := map[string]any{"$set": map[string]any{}}
	for k, v := range edit {
		if k != "start_time" && k != "spec_file" && k != "scan_number" {
			update_spec["$set"].(map[string]any)[k] = v
		}
	}
	err = mongo.UpsertRecord(
		srvConfig.Config.SpecScans.MongoDB.DBName,
		srvConfig.Config.SpecScans.MongoDB.DBColl,
		query,
		update_spec,
	)
	if err != nil {
		err_ch <- err
		return
	}
	rec_ch <- edited_record
}

func validateRecord(record any) (bool, error) {
	var record_map map[string]any
	switch record.(type) {
	case map[string]any:
		record_map = record.(map[string]any)
	default:
		err := mapstructure.Decode(record, &record_map)
		if err != nil {
			return false, err
		}
	}
	err := lexicon.ValidateRecord(record_map)
	if err != nil {
		return false, err
	}
	err = Schema.Validate(record_map)
	if err != nil {
		return false, err
	}
	return true, nil
}

// Returns map of queries for a single service sorted by the query
// keys' DBType
func getServiceQueriesByDBType(q ql.QLManager, servicename string, query string) (map[string]map[string]any, error) {
	dbqueries := make(map[string]map[string]any)
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
					dbqueries[rec.DBType] = map[string]any{key: val}
				}
			}
		}
	}
	return dbqueries, nil
}

// Get matching records from the mongodb only
func getMongoRecords(query map[string]any, idx int, limit int) ([]MongoRecord, error) {
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

func getMotorRecords(query map[string]any) ([]MotorRecord, error) {
	motor_records := QueryMotorsDb(query)
	if Verbose > 0 {
		log.Printf("query %v found %d records\n", query, len(motor_records))
	}
	return motor_records, nil
}
