package main

import (
	"log"
	"time"

	schema "github.com/CHESSComputing/golib/beamlines"
	srvConfig "github.com/CHESSComputing/golib/config"
	mongo "github.com/CHESSComputing/golib/mongo"
	mapstructure "github.com/mitchellh/mapstructure"
)

var Schema *schema.Schema

type UserRecord struct {
	ScanId      float64            `json:"sid,omitempty" mapstructure:"did,omitempty"`
	DatasetId   string             `json:"did" mapstructure:"did"`
	Cycle       string             `json:"cycle" mapstructure:"cycle"`
	Beamline    string             `json:"beamline" mapstructure:"beamline"`
	Btr         string             `json:"btr" mapstructure:"btr"`
	SpecFile    string             `json:"spec_file" mapstructure:"spec_file"`
	ScanNumber  int8               `json:"scan_number" mapstructure:"scan_number"`
	StartTime   float64            `json:"start_time" mapstructure:"start_time"`
	Command     string             `json:"command" mapstructure:"command"`
	Status      string             `json:"status" mapstructure:"status"`
	Comments    []string           `json:"comments" mapstructure:"comments"`
	SpecVersion string             `json:"spec_version" mapstructure:"spec_version"`
	Motors      map[string]float64 `json:"motors" mapstructure:"motors"`
	Variables   map[string]any     `json:"variables" mapstructure:"variables"`
}

type MongoRecord struct {
	ScanId      float64        `mapstructure:"sid"`
	DatasetId   string         `mapstructure:"did"`
	Cycle       string         `mapstructure:"cycle"`
	Beamline    string         `mapstructure:"beamline"`
	Btr         string         `mapstructure:"btr"`
	SpecFile    string         `mapstructure:"spec_file"`
	ScanNumber  int8           `mapstructure:"scan_number"`
	StartTime   float64        `mapstructure:"start_time"`
	Command     string         `mapstructure:"command"`
	Status      string         `mapstructure:"status"`
	Comments    []string       `mapstructure:"comments"`
	SpecVersion string         `mapstructure:"spec_version"`
	Variables   map[string]any `mapstructure:"variables"`
}

func InitSchemaManager() {
	var smgr schema.SchemaManager
	_schema, err := smgr.Load(srvConfig.Config.SpecScans.SchemaFile)
	if err != nil {
		log.Println("Problem loading schema")
	} else {
		Schema = _schema
	}
	log.Printf("schema: %v", Schema)
}

// Decompose a user-submitted scan record into two portions: the portion to
// reside in the MongoDB, and the motor positions (which will reside in the SQL
// db).
func DecomposeRecord(user_record UserRecord) (MongoRecord, MotorRecord) {
	var scan_id float64
	if user_record.ScanId < 0 {
		// This is a test record, so force a unique scan id
		scan_id = float64(time.Now().UnixNano()) / 1e9
	} else {
		scan_id = user_record.StartTime
	}
	mongo_record := MongoRecord{
		ScanId:      scan_id,
		DatasetId:   user_record.DatasetId,
		Cycle:       user_record.Cycle,
		Beamline:    user_record.Beamline,
		Btr:         user_record.Btr,
		SpecFile:    user_record.SpecFile,
		ScanNumber:  user_record.ScanNumber,
		StartTime:   user_record.StartTime,
		Command:     user_record.Command,
		Status:      user_record.Status,
		Comments:    user_record.Comments,
		SpecVersion: user_record.SpecVersion,
		Variables:   user_record.Variables,
	}
	motor_record := MotorRecord{
		ScanId: scan_id,
		Motors: user_record.Motors,
	}
	return mongo_record, motor_record
}

// Combine a partial scan record with its motor positions, return the completed record
func CompleteRecord(mongo_record MongoRecord, motor_record MotorRecord) UserRecord {
	record := UserRecord{
		ScanId:      mongo_record.ScanId,
		DatasetId:   mongo_record.DatasetId,
		Cycle:       mongo_record.Cycle,
		Beamline:    mongo_record.Beamline,
		Btr:         mongo_record.Btr,
		SpecFile:    mongo_record.SpecFile,
		ScanNumber:  mongo_record.ScanNumber,
		StartTime:   mongo_record.StartTime,
		Command:     mongo_record.Command,
		Status:      mongo_record.Status,
		Comments:    mongo_record.Comments,
		SpecVersion: mongo_record.SpecVersion,
		Motors:      motor_record.Motors,
		Variables:   mongo_record.Variables,
	}
	return record
}

// Return the completed UserRecords corresponding to the MongoRecords provided
func CompleteMongoRecords(mongo_records ...MongoRecord) ([]UserRecord, error) {
	var user_records []UserRecord
	if len(mongo_records) == 0 {
		return user_records, nil
	}
	var sids []float64
	for _, mongo_record := range mongo_records {
		sids = append(sids, mongo_record.ScanId)
	}
	motor_records, err := GetMotorRecords(sids...)
	if err != nil {
		return user_records, err
	}
	for _, mongo_record := range mongo_records {
		for _, motor_record := range motor_records {
			if motor_record.ScanId == mongo_record.ScanId {
				user_records = append(user_records, CompleteRecord(mongo_record, motor_record))
			}
		}
	}
	return user_records, nil
}

// Return the completed UserRecords correcponding to the MotorRecords provided
func CompleteMotorRecords(motor_records ...MotorRecord) ([]UserRecord, error) {
	var user_records []UserRecord
	if len(motor_records) == 0 {
		return user_records, nil
	}
	var sids []float64
	for _, motor_record := range motor_records {
		sids = append(sids, motor_record.ScanId)
	}
	mongo_query := map[string]any{"sid": map[string]any{"$in": sids}}
	mongo_records := mongo.Get(srvConfig.Config.SpecScans.MongoDB.DBName, srvConfig.Config.SpecScans.MongoDB.DBColl, mongo_query, 0, 0)
	for _, mongo_record_map := range mongo_records {
		var mongo_record MongoRecord
		err := mapstructure.Decode(mongo_record_map, &mongo_record)
		if err != nil {
			return user_records, err
		}
		for _, motor_record := range motor_records {
			if motor_record.ScanId == mongo_record.ScanId {
				user_records = append(user_records, CompleteRecord(mongo_record, motor_record))
				break
			}
		}
	}
	return user_records, nil
}

// Return the completed UserRecords that can be constructed from the MongoRecords
// and MotorRecords provided (CanIds of returned UserRecords will be the
// intersection of the ScanIds of both sets)
func getIntersectionRecords(mongo_records []MongoRecord, motor_records []MotorRecord) []UserRecord {
	var user_records []UserRecord
	for _, mongo_record := range mongo_records {
		for _, motor_record := range motor_records {
			if mongo_record.ScanId == motor_record.ScanId {
				user_records = append(user_records, CompleteRecord(mongo_record, motor_record))
			}
		}
	}
	return user_records
}
