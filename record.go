package main

import (
	"log"

	schema "github.com/CHESSComputing/golib/beamlines"
	srvConfig "github.com/CHESSComputing/golib/config"
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
}

type MongoRecord struct {
	ScanId      float64  `mapstructure:"sid"`
	DatasetId   string   `mapstructure:"did"`
	Cycle       string   `mapstructure:"cycle"`
	Beamline    string   `mapstructure:"beamline"`
	Btr         string   `mapstructure:"btr"`
	SpecFile    string   `mapstructure:"spec_file"`
	ScanNumber  int8     `mapstructure:"scan_number"`
	StartTime   float64  `mapstructure:"start_time"`
	Command     string   `mapstructure:"command"`
	Status      string   `mapstructure:"status"`
	Comments    []string `mapstructure:"comments"`
	SpecVersion string   `mapstructure:"spec_version"`
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
	mongo_record := MongoRecord{
		ScanId:      user_record.StartTime,
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
	}
	motor_record := MotorRecord{
		ScanId: user_record.StartTime,
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
	}
	return record
}
