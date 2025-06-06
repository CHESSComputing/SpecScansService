package main

import (
	"bytes"
	"database/sql"
	"errors"
	"log"
	"path"
	"regexp"
	"strings"
	"text/template"

	srvConfig "github.com/CHESSComputing/golib/config"
	sqldb "github.com/CHESSComputing/golib/sqldb"
)

// var MotorsDb is our database pointer
var MotorsDb *sql.DB

type MotorRecord struct {
	ScanId string
	Motors map[string]float64
}

type MotorPositionQuery struct {
	Mne   string
	Exact []float64
	Min   float64
	Max   float64
}
type MotorsDbQuery struct {
	Sids                 []string
	MotorPositionQueries []MotorPositionQuery
}

func InitMotorsDb() {
	dbtype, dburi, dbowner := sqldb.ParseDBFile(srvConfig.Config.SpecScans.DBFile)
	log.Printf("InitDB: type=%s owner=%s", dbtype, dbowner)
	db, err := sqldb.InitDB(dbtype, dburi)
	if err != nil {
		log.Fatal(err)
	}
	MotorsDb = db
}

func InsertMotors(r MotorRecord, db *sql.DB) (int64, error) {
	tx, err := db.Begin()
	if err != nil {
		return -1, err
	}
	defer tx.Rollback()

	// Insert the given motor record to the three tables that compose the static
	// motor positions database.
	log.Printf("Inserting motor record: %v", r)
	result, err := tx.Exec("INSERT INTO ScanIds (sid) VALUES (?)", r.ScanId)
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
	for mne, pos := range r.Motors {
		result, err = tx.Exec("INSERT INTO MotorMnes (motor_mne) VALUES (?)", mne)
		if err != nil {
			err = tx.QueryRow("SELECT motor_id FROM MotorMnes WHERE motor_mne=?", mne).Scan(&motor_id)
			if err != nil {
				log.Printf("Could not insert record to MotorMnes table; error: %v", err)
				continue
			}
		} else {
			motor_id, err = result.LastInsertId()
		}
		if err != nil {
			log.Printf("Could not get ID of new record in MotorMnes; error: %v", err)
			continue
		}
		result, err = tx.Exec("INSERT INTO MotorPositions (scan_id, motor_id, motor_position) VALUES (?, ?, ?)", scan_id, motor_id, pos)
		if err != nil {
			log.Printf("Could not insert record to MotorPositions table; error: %v", err)
		}
	}
	err = tx.Commit()
	return scan_id, err
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

func GetMotorRecords(sids ...string) ([]MotorRecord, error) {
	query := MotorsDbQuery{Sids: sids}
	return queryMotorsDb(query), nil
}

func QueryMotorsDb(query map[string]any) []MotorRecord {
	motorsdb_query := translateQuery(query)
	if Verbose > 0 {
		log.Printf("motorsdb_query: %+v\n", motorsdb_query)
	}
	return queryMotorsDb(motorsdb_query)
}

func translateQuery(query map[string]any) MotorsDbQuery {
	var motorsdb_query MotorsDbQuery

	// Consolidate values from user query keys like "motors" and "motors.*" so
	// that we have a query map where keys are motor names only.
	var position_queries []map[string]any
	for key, val := range query {
		if key == "motors" {
			for _key, _val := range val.(map[string]any) {
				position_queries = append(position_queries, map[string]any{_key: _val})
			}
		} else {
			queryKey := strings.TrimPrefix(key, "motors.")
			position_queries = append(position_queries, map[string]any{queryKey: val})
		}
	}
	for _, v := range position_queries {
		motorsdb_query.MotorPositionQueries = append(motorsdb_query.MotorPositionQueries, translatePositionQuery(v))
	}
	return motorsdb_query
}

func translatePositionQuery(query any) MotorPositionQuery {
	var position_query MotorPositionQuery
	switch query.(type) {
	case string:
		position_query.Mne = query.(string)
	case map[string]any:
		for k, v := range query.(map[string]any) {
			position_query.Mne = k
			switch v.(type) {
			case float64, float32:
				position_query.Exact = []float64{v.(float64)}
			case []any:
				for _, pos := range v.([]any) {
					position_query.Exact = append(position_query.Exact, pos.(float64))
				}
				// log.Printf("value is []any: %v (%T)", v, v)
				// position_query.Exact = v.([]float64)
			case map[string]any:
				for kk, vv := range v.(map[string]any) {
					if kk == "$lt" {
						position_query.Max = vv.(float64)
					} else if kk == "$gt" {
						position_query.Min = vv.(float64)
					} else if kk == "$in" {
						for _, pos := range vv.([]any) {
							position_query.Exact = append(position_query.Exact, pos.(float64))
						}
					} else if kk == "$eq" {
						position_query.Exact = []float64{vv.(float64)}
					}
				}
			}
		}
	}
	return position_query
}

func queryMotorsDb(query MotorsDbQuery) []MotorRecord {
	var motor_records []MotorRecord
	statement, err := getSqlStatement("query_motorsdb.sql", query)
	if err != nil {
		log.Printf("Could not get appropriate SQL query statement; error: %v", err)
		return motor_records
	}
	if Verbose > 1 {
		log.Printf("Motors db query SQL statement: %s", statement)
	}
	rows, err := MotorsDb.Query(statement)
	if err != nil {
		log.Printf("Could not query motor positions database; error: %v", err)
		return motor_records
	}
	return parseMotorRecords(rows)
}

func parseMotorRecords(rows *sql.Rows) []MotorRecord {
	// Helper for parsing non-grouped results of sql query
	var motor_records []MotorRecord
	record_map := make(map[string]map[string]float64) // map of scan ids: map of motor mne: position
	var sid string
	var motor_mne string
	var motor_pos float64
	for rows.Next() {
		err := rows.Scan(&sid, &motor_mne, &motor_pos)
		if err != nil {
			log.Printf("Could not parse row of results: %v\n", err)
			continue
		}
		_, ok := record_map[sid]
		if !ok {
			record_map[sid] = make(map[string]float64)
		}
		record_map[sid][motor_mne] = motor_pos
	}
	for sid := range record_map {
		motor_records = append(motor_records, MotorRecord{ScanId: sid, Motors: record_map[sid]})
	}
	return motor_records
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
	// Trim whitespace for certain sql dirver(s)
	statement = strings.TrimSpace(statement)
	re := regexp.MustCompile(`\s+`)
	statement = string(re.ReplaceAll([]byte(statement), []byte(" ")))
	return statement, nil
}
