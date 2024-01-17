package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"

	primitive "go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	auth "github.com/CHESSComputing/golib/authz"
	srvConfig "github.com/CHESSComputing/golib/config"
)

// Helper for handling errors
func HTTPError(label, msg string, w http.ResponseWriter) {
	log.Println(label, msg)
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(msg))
	return
}

func GinError(c *gin.Context, msg string) {
	c.Error(errors.New(msg))
}
func GinErrorFrom(c *gin.Context, msg string, err error) {
	c.Error(err)
	c.Error(errors.New(msg))
}

// Handler for adding a new record to the database
func AddHandler(c *gin.Context) {
	var record Record
	if err := c.Bind(&record); err != nil {
		GinErrorFrom(c, "Cannot bind request body to Record", err)
		c.String(http.StatusBadRequest, "Bad request")
	}

	// Peel of motor mnemonics & positions -- these are submitted to an rdb, not
	// the mongodb.
	motor_record := MotorRecord{MotorMnes: record.MotorMnes, MotorPositions: record.MotorPositions}
	record.MotorMnes = nil
	record.MotorPositions = nil

	// Connect to MongoDb
	log.Printf("Connecting to %s", srvConfig.Config.MetaData.MongoDB.DBUri)                                          // FIX temporary config
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(srvConfig.Config.MetaData.MongoDB.DBUri)) // FIX temporary config
	if err != nil {
		GinErrorFrom(c, "Cannot connect to database", err)
		return
	}
	defer func() {
		if err = client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()
	// Get the Mongodb collection of interest and insert the record
	coll := client.Database(srvConfig.Config.MetaData.MongoDB.DBName).Collection(srvConfig.Config.MetaData.MongoDB.DBColl) // FIX temporary config
	result, err := coll.InsertOne(context.TODO(), &record)
	if err != nil {
		GinErrorFrom(c, "Cannot insert record to mongodb", err)
		return
	}
	rec_id := result.InsertedID.(primitive.ObjectID).Hex()
	log.Printf("Added record to mongodb: %v (ID: %v)\n", record, rec_id)

	// Now insert the motor mnes & positions record
	motor_record.DatasetId = rec_id
	// NB: mongoid and sqlid should be the same
	sql_id, err := InsertMotors(motor_record)
	if err != nil {
		GinErrorFrom(c, "Cannot insert motor positions record", err)
		return
	}
	log.Printf("Added record to SQL db: %v (ID: %v)\n", motor_record, sql_id)

	c.String(http.StatusOK, fmt.Sprintf("New record ID: %s\n", rec_id))
	return
}

// Handler for editing a record already in the database
func EditHandler(c *gin.Context) {
	// Get ID of record to edit
	id := c.Param("id")
	if id == "" {
		GinError(c, "No record id in form")
		return
	}
	// Ensure requesting user is allowed to edit this record
	username, err := auth.UserCredentials(c.Request)
	if err != nil {
		GinErrorFrom(c, "Cannot determine username", err)
		return
	}
	// Check with BeamPass: user must associated with the BTR of this record
	log.Printf("User %s attempting to edit record %s\n", username, id)
	log.Printf("Placeholder: Update record %s in the database", id)
	c.String(http.StatusOK, fmt.Sprintf("Record %s updated", id))
	return
}

// Handler for querying the database for records
func SearchHandler(c *gin.Context) {
	// Perform search using parameters from the form;
	// get slice of matching Records
	data, err := c.GetRawData()
	if err != nil {
		GinErrorFrom(c, "Cannot get query data", err)
		return
	}
	var selectors struct{}
	if err := json.Unmarshal(data, &selectors); err != nil {
		GinErrorFrom(c, "Cannot decode body of request as JSON", err)
	}

	log.Printf("Placeholder: Search database with selectors: %v\n", selectors)
	var records []Record
	log.Printf("Matching records: %v\n", records)
	c.JSON(http.StatusOK, records)
	return
}
