package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"

	structtomap "github.com/Klathmon/StructToMap"
	"github.com/gin-gonic/gin"
	bson "go.mongodb.org/mongo-driver/bson"

	auth "github.com/CHESSComputing/golib/authz"
	srvConfig "github.com/CHESSComputing/golib/config"
	mongo "github.com/CHESSComputing/golib/mongo"
	services "github.com/CHESSComputing/golib/services"
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
	record_map, err := structtomap.Convert(record)
	if err != nil {
		resp := services.Response("SpecScans", http.StatusInternalServerError, services.ParseError, err)
		c.JSON(http.StatusInternalServerError, resp)
		return
	}

	// Get the ScanId and add it to the records to be submitted
	sid := uint64(record_map["StartTime"].(float64))
	record_map["ScanId"] = sid
	log.Printf("New record ScanId: %d", sid)
	motor_record := MotorRecord{
		ScanId: sid,
		Motors: record.Motors}

	log.Printf("New record: %v", record)
	// Peel off motor mnemonics & positions -- these are submitted to an rdb, not
	// the mongodb.
	record_map["Motors"] = nil

	// Submit one portion of the record to mongodb...
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

	//     c.String(http.StatusOK, fmt.Sprintf("New record ScanId: %d\n", sid))
	resp := services.Response("SpecScans", http.StatusOK, services.OK, nil)
	resp.Results = services.ServiceResults{NRecords: len(mongo_records), Records: mongo_records}
	c.JSON(http.StatusOK, resp)
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
	log.Printf("QLM: %+v", QLM)
	log.Printf("service request: %+v", query_request)

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
	log.Printf("queries %+v", queries)
	log.Printf("Mongo query: %v", queries["Mongo"])
	log.Printf("SQL query: %v", queries["SQL"])

	// Query the mongodb
	var records []map[string]any
	nrecords := 0
	if queries["Mongo"] != nil {
		nrecords = mongo.Count(srvConfig.Config.SpecScans.MongoDB.DBName, srvConfig.Config.SpecScans.MongoDB.DBColl, queries["Mongo"])
		records = mongo.Get(srvConfig.Config.SpecScans.MongoDB.DBName, srvConfig.Config.SpecScans.MongoDB.DBColl, queries["Mongo"], idx, limit)
		if Verbose > 0 {
			log.Printf("spec %v nrecords %d return idx=%d limit=%d", queries["Mongo"], nrecords, idx, limit)
		}
	}

	// Query the SQL db of motor positions
	if queries["SQL"] != nil {
		motor_records := QueryMotorsDb(queries["SQL"]["motors"])

		if queries["Mongo"] != nil {
			// Aggregate intersection of results from both dbs
			var intersection_records []map[string]any
			for _, record := range records {
				sid := uint64(record["ScanId"].(int64))
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
			mongo_query := bson.M{"ScanId": bson.M{"$in": sids}}
			mongo_records := mongo.Get(srvConfig.Config.SpecScans.MongoDB.DBName, srvConfig.Config.SpecScans.MongoDB.DBColl, mongo_query, idx, limit)
			for _, mongo_record := range mongo_records {
				for _, motor_record := range motor_records {
					if motor_record.ScanId == uint64(mongo_record["ScanId"].(int64)) {
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
			sids = append(sids, uint64(record["ScanId"].(int64)))
		}
		motor_records, err := GetMotorRecords(sids...)
		if err != nil {
			resp := services.Response("SpecScans", http.StatusInternalServerError, services.DatabaseError, err)
			c.JSON(http.StatusInternalServerError, resp)
			return
		}
		for i, record := range records {
			for _, motor_record := range motor_records {
				if motor_record.ScanId == uint64(record["ScanId"].(int64)) {
					records[i] = CompleteRecord(record, motor_record)
				}
			}
		}

	}

	c.JSON(http.StatusOK, records)
	return
}
