package main

import (
	"database/sql"
	"log"
	"sync"

	_ "github.com/mattn/go-sqlite3"

	srvConfig "github.com/CHESSComputing/golib/config"
)

// Safe to use concurrently
type SafeDb struct {
	db *sql.DB
	mu sync.Mutex
}

var MotorsDb SafeDb

type MotorRecord struct {
	DatasetId      string
	MotorMnes      []string
	MotorPositions []float64
}

func InitMotorsDb() {
	db, err := sql.Open("sqlite3", srvConfig.Config.SpecScans.DBFile)
	if err != nil {
		log.Fatal(err)
	}
	// Test db accessibility
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Pinged motors db")

	MotorsDb = SafeDb{db: db}
}

func InsertMotors(r MotorRecord) (int64, error) {
	// Insert the given motor record to the three tables that compose the static
	// motor positions database.
	log.Printf("Inserting motor record: %v", r)
	result, err := MotorsDb.db.Exec("INSERT INTO DID (did) VALUES (?)", r.DatasetId)
	if err != nil {
		log.Printf("Could not insert record to DID table; error: %v", err)
		return -1, err
	}
	dataset_id, err := result.LastInsertId()
	if err != nil {
		log.Printf("Could not get ID of new record in DID; error: %v", err)
		return dataset_id, err
	}
	var motor_id int64
	for i := 0; i < len(r.MotorMnes); i++ {
		result, err = MotorsDb.db.Exec("INSERT INTO MotorMnes (dataset_id, motor_mne) VALUES (?, ?)", dataset_id, r.MotorMnes[i])
		if err != nil {
			log.Printf("Could not insert record to MotorMnes table; error: %v", err)
			continue;
		}
		motor_id, err = result.LastInsertId()
		if err != nil {
			log.Printf("Could not get ID of new record in MotorMnes; error: %v", err)
			continue;
		}
		result, err = MotorsDb.db.Exec("INSERT INTO MotorPositions (motor_id, motor_position) VALUES (?, ?)", motor_id, r.MotorPositions[i])
		if err != nil {
			log.Printf("Could not insert record to MotorPositions table; error: %v", err)
		}
	}
	return dataset_id, nil
}

func QueryMotorPosition(mne string, pos float64) []MotorRecord {
	// Return a slice of complete motor position records for all the datasets
	// which included the given motor mnemonic and match at the given position.
	var motor_records []MotorRecord
	rows, err := MotorsDb.db.Query("SELECT D.did group_concat(M.motor_mne), group_concat(P.motor_position) FROM MotorPositions as P JOIN MotorMnes AS M ON M.motor_id=P.motor_id JOIN DID AS D ON D.dataset_id=M.dataset_id WHERE M.Motor_mne=? AND P.motor_position=? GROUP BY D.did", mne, pos)
	var motor_record MotorRecord
	for rows.Next() {
		err = rows.Scan(&motor_record.DatasetId, &motor_record.MotorMnes, &motor_record.MotorPositions)
		if err != nil {
			log.Printf("Could not get a motor positions record from a row of SQL results. error: %v", err)
		}
	}
	return motor_records
}
