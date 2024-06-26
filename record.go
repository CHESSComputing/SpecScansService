package main

import (
	"log"

	schema "github.com/CHESSComputing/golib/beamlines"
	srvConfig "github.com/CHESSComputing/golib/config"
)

var Schema *schema.Schema

func InitSchemaManager() {
	var smgr schema.SchemaManager
	// TODO: add field for schema file in golib/config.SpecScans, then use
	// srvConfig.Config.SpecScans.Schema as the arg to smgr.Load
	// (use of srvConfig.Config.SpecScans.WebServer.MetricsPrefix) is just temporary)
	_schema, err := smgr.Load(srvConfig.Config.SpecScans.WebServer.MetricsPrefix)
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
func DecomposeRecord(record map[string]any) (map[string]any, MotorRecord) {
	// Complete the Mongodb portion with ScanId (derived from the scan's start time).
	sid := uint64(record["start_time"].(float64))
	record["sid"] = sid

	// Separate motor positions from teh user-submitted record.
	motors := make(map[string]float64)
	for k, v := range record["motors"].(map[string]interface{}) {
		motors[k] = v.(float64)
	}
	motor_record := MotorRecord{
		ScanId: sid,
		Motors: motors}
	delete(record, "motors")

	return record, motor_record
}

// Combine a partial scan record with its motor positions, return the completed record
func CompleteRecord(record map[string]any, motor_record MotorRecord) map[string]any {
	record["motors"] = motor_record.Motors
	return record
}
