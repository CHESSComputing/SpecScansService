package main

import (
	"bytes"
	"database/sql"
	"errors"
	"log"
	"path"
	"strconv"
	"strings"
	"sync"
	"text/template"

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
	ScanId         uint64
	MotorMnes      []string
	MotorPositions []float64
}

type MotorPositionQuery struct {
	Mne   string
	Exact []float64
	Min   float64
	Max   float64
}
type MotorsDbQuery struct {
	Sids                 []uint64
	MotorPositionQueries []MotorPositionQuery
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
	result, err := MotorsDb.db.Exec("INSERT INTO ScanIds (sid) VALUES (?)", r.ScanId)
	if err != nil {
		log.Printf("Could not insert record to ScanIds table; error: %v", err)
		return -1, err
	}
	scan_id, err := result.LastInsertId()
	if err != nil {
		log.Printf("Could not get ID of new record in ScanIds; error: %v", err)
		return scan_id, err
	}
	var motor_id int64
	for i := 0; i < len(r.MotorMnes); i++ {
		result, err = MotorsDb.db.Exec("INSERT INTO MotorMnes (scan_id, motor_mne) VALUES (?, ?)", scan_id, r.MotorMnes[i])
		if err != nil {
			log.Printf("Could not insert record to MotorMnes table; error: %v", err)
			continue
		}
		motor_id, err = result.LastInsertId()
		if err != nil {
			log.Printf("Could not get ID of new record in MotorMnes; error: %v", err)
			continue
		}
		result, err = MotorsDb.db.Exec("INSERT INTO MotorPositions (motor_id, motor_position) VALUES (?, ?)", motor_id, r.MotorPositions[i])
		if err != nil {
			log.Printf("Could not insert record to MotorPositions table; error: %v", err)
		}
	}
	return scan_id, nil
}

func QueryMotorPosition(mne string, pos float64) []MotorRecord {
	query := MotorsDbQuery{
		MotorPositionQueries: []MotorPositionQuery{
			MotorPositionQuery{
				Mne:   mne,
				Exact: []float64{pos},
			},
		},
	}
	return queryMotorsDb(query)
}

func GetMotorRecord(sid uint64) (MotorRecord, error) {
	query := MotorsDbQuery{Sids: []uint64{sid}}
	return queryMotorsDb(query)[0], nil
}

func queryMotorsDb(query MotorsDbQuery) []MotorRecord {
	var motor_records []MotorRecord
	statement, err := getSqlStatement("query_motorsdb.sql", query)
	if err != nil {
		log.Printf("Could not get appropriate SQL query statement; error: %v", err)
		return motor_records
	}
	rows, err := MotorsDb.db.Query(statement)
	if err != nil {
		log.Printf("Could not query motor positions database; error: %v", err)
		return motor_records
	}
	return getMotorRecords(rows)
}

func getMotorRecords(rows *sql.Rows) []MotorRecord {
	// Helper for parsing grouped results of sql query
	var motor_records []MotorRecord
	// Parse the first record;
	// need to do this outside the loop if there is only one row of results.
	rows.Next()
	motor_record := getMotorRecord(rows)
	motor_records = append(motor_records, motor_record)
	for rows.Next() {
		motor_record := getMotorRecord(rows)
		motor_records = append(motor_records, motor_record)
	}
	return motor_records
}

func getMotorRecord(rows *sql.Rows) MotorRecord {
	// Helper for parsing grouped results of sql query at the current cursor position only
	motor_record := MotorRecord{}
	_motor_mnes, _motor_positions := "", ""
	err := rows.Scan(&motor_record.ScanId, &_motor_mnes, &_motor_positions)
	if err != nil {
		log.Printf("Could not get a MotorRecord from a row of SQL results. error: %v", err)
		return motor_record
	}
	motor_record.MotorMnes = strings.Split(_motor_mnes, ",")
	motor_positions := make([]float64, 0, len(motor_record.MotorMnes))
	for _, position := range strings.Split(_motor_positions, ",") {
		position, _ := strconv.ParseFloat(position, 64)
		motor_positions = append(motor_positions, position)
	}
	motor_record.MotorPositions = motor_positions
	return motor_record
}

func getSqlStatement(tmpl_file string, params MotorsDbQuery) (string, error) {
	tmpl, err := template.New(tmpl_file).ParseFiles(path.Join(srvConfig.Config.SpecScans.WebServer.StaticDir, tmpl_file))
	if err != nil {
		return "", err
	}
	statement := ""
	buf := bytes.NewBufferString(statement)
	err = tmpl.Execute(buf, params)
	if err != nil {
		return "", err
	}
	statement = buf.String()
	if statement == "" {
		return "", errors.New("Statement is empty")
	}
	return statement, nil
}
