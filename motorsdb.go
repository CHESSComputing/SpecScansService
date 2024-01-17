package main

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
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
	db, err := sql.Open("sqlite3", srvConfig.Config.MetaData.WebServer.LimiterHeader) // FIX temporary config -- just use LimiterHeader since it's a string field that I'm not using otherwise
	if err != nil {
		log.Fatal(err)
	}
	// Test db accessibility
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Pinged motors db")

	// Create motors table if needed
	_, err = db.Exec(`
CREATE TABLE IF NOT EXISTS motors (
DatasetId      TEXT                NOT NULL PRIMARY KEY,
MotorMnes      TEXT VARARRAY[100]  NOT NULL,
MotorPositions FLOAT VARARRAY[100] NOT NULL
);`)
	if err != nil {
		log.Println(err)
	} else {
		log.Println("motorsdb motors table exists")
	}
	MotorsDb = SafeDb{db: db}
}

func InsertMotors(r MotorRecord) (int, error) {
	log.Printf("Inserting motor record: %v", r)
	// Format array data types for inserting w/ SQL syntax
	motor_mnes := []string{}
	motor_positions := []string{}
	for i := 0; i < len(r.MotorMnes); i++ {
		motor_mnes = append(motor_mnes, "\""+r.MotorMnes[i]+"\"")
		motor_positions = append(motor_positions, strconv.FormatFloat(r.MotorPositions[i], 'f', -1, 64))
	}
	motor_mnes_value := "'{" + strings.Join(motor_mnes, ",") + "}'"
	motor_positions_value := "'{" + strings.Join(motor_positions, ",") + "}'"
	exec_command := fmt.Sprintf("INSERT INTO motors VALUES(\"%s\",%s,%s);\n", r.DatasetId, motor_mnes_value, motor_positions_value)
	res, err := MotorsDb.db.Exec(exec_command)
	if err != nil {
		log.Printf("Could not insert record: %v", r)
		return 0, err
	}
	var id int64
	if id, err = res.LastInsertId(); err != nil {
		return 0, err
	}
	return int(id), nil
}
