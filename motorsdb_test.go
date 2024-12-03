package main

import (
	"database/sql"
	"fmt"
	"log"
	"sort"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// SetupTestDB sets up an in-memory SQLite database for testing.
func SetupTestDB(t *testing.T) *sql.DB {
	// Open an in-memory SQLite database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open in-memory SQLite database: %v", err)
	}

	// Create tables
	createTableQuery := `
  CREATE TABLE IF NOT EXISTS ScanIds (
  scan_id INTEGER PRIMARY KEY AUTOINCREMENT,
  sid FLOAT(8) NOT NULL UNIQUE
  );

  CREATE TABLE IF NOT EXISTS MotorMnes (
  motor_id INTEGER PRIMARY KEY AUTOINCREMENT,
  scan_id INTEGER NOT NULL,
  motor_mne VARCHAR(255) NOT NULL
  );

  CREATE TABLE IF NOT EXISTS MotorPositions (
  motor_id INTEGER NOT NULL,
  motor_position FLOAT
  );`
	_, err = db.Exec(createTableQuery)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	return db
}

// TestInsertMotors tests the InsertMotors function using an in-memory database
func TestInsertMotors(t *testing.T) {
	// Set up the in-memory database
	db := SetupTestDB(t)
	defer db.Close()

	// Set up table of test records to submit
	tests := []struct {
		motor_record   MotorRecord
		expect_success bool
	}{
		{
			MotorRecord{
				ScanId: 0.0,
				Motors: map[string]float64{
					"mne0": 1.23,
					"mne1": 4.56,
				},
			},
			true,
		},
		{
			MotorRecord{
				ScanId: 0.0, // violates unique sid constraint
				Motors: map[string]float64{},
			},
			false,
		},
		{
			MotorRecord{
				ScanId: 1.0,
				Motors: map[string]float64{
					"mne0": 1.23,
					"mne1": 4.56,
				},
			},
			true,
		},
	}

	// Run tests
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			scan_id, err := InsertMotors(tt.motor_record, db)
			if err != nil && tt.expect_success {
				t.Errorf("InsertMotors(%v, db) failed", tt.motor_record)
			}
			if tt.expect_success {
				err := validateMotorsDbRowCounts(tt.motor_record, scan_id, db)
				if err != nil {
					t.Error(err)
				}
			}
		})
	}
}

// Helper to validate the number of rows belonging to a given record in each table of the motors db.
func validateMotorsDbRowCounts(motor_record MotorRecord, scan_id int64, db *sql.DB) error {
	var count int

	err := db.QueryRow("SELECT COUNT(*) FROM ScanIds WHERE sid = ?", motor_record.ScanId).Scan(&count)
	if err != nil {
		return fmt.Errorf("Failed to query ScanIds table: %v", err)
	}
	if count != 1 {
		return fmt.Errorf("Expected 1 record in ScanIds, but got %d", count)
	}

	err = db.QueryRow("SELECT COUNT(*) FROM MotorMnes WHERE scan_id = ?", scan_id).Scan(&count)
	if err != nil {
		return fmt.Errorf("Failed to query MotorMnes table: %v", err)
	}
	if count != len(motor_record.Motors) {
		return fmt.Errorf("Expected %d records in MotorMnes, but got %d", len(motor_record.Motors), count)
	}

	//err = db.QueryRow("SELECT COUNT(*) FROM MotorPositions").Scan(&count)
	err = db.QueryRow("SELECT COUNT(*) FROM MotorPositions mp JOIN MotorMnes mm ON mp.motor_id = mm.motor_id WHERE mm.scan_id = ?", scan_id).Scan(&count)
	if err != nil {
		return fmt.Errorf("Failed to query MotorPositions table: %v", err)
	}
	if count != len(motor_record.Motors) {
		return fmt.Errorf("Expected %d records in MotorPositions, but got %d", len(motor_record.Motors), count)
	}

	return nil
}

// Test for translating "mototrs" portions of user queries to
// MotorsDbQuery structs
func TestTranslateQuery(t *testing.T) {
	tests := []struct {
		query    map[string]any
		expected MotorsDbQuery
	}{
		{
			query: map[string]any{"motors": map[string]any{"mne": 1.23}},
			expected: MotorsDbQuery{
				MotorPositionQueries: []MotorPositionQuery{
					MotorPositionQuery{
						Mne:   "mne",
						Exact: []float64{1.23},
					},
				},
			},
		},
		{
			query: map[string]any{"motors": map[string]any{"mne0": 1.23}, "motors.mne1": 4.56},
			expected: MotorsDbQuery{
				MotorPositionQueries: []MotorPositionQuery{
					MotorPositionQuery{
						Mne:   "mne0",
						Exact: []float64{1.23},
					},
					MotorPositionQuery{
						Mne:   "mne1",
						Exact: []float64{4.56},
					},
				},
			},
		},
		{
			query: map[string]any{
				"motors.mne": 1.23,
			},
			expected: MotorsDbQuery{
				MotorPositionQueries: []MotorPositionQuery{
					MotorPositionQuery{
						Mne:   "mne",
						Exact: []float64{1.23},
					},
				},
			},
		},
		{
			query: map[string]any{
				"motors.mne": map[string]any{"$lt": 1.23},
			},
			expected: MotorsDbQuery{
				MotorPositionQueries: []MotorPositionQuery{
					MotorPositionQuery{
						Mne: "mne",
						Max: 1.23,
					},
				},
			},
		},
		{
			query: map[string]any{
				"motors.mne": map[string]any{"$gt": 1.23},
			},
			expected: MotorsDbQuery{
				MotorPositionQueries: []MotorPositionQuery{
					MotorPositionQuery{
						Mne: "mne",
						Min: 1.23,
					},
				},
			},
		},
		{
			query: map[string]any{
				"motors.mne": map[string]any{"$eq": 1.23},
			},
			expected: MotorsDbQuery{
				MotorPositionQueries: []MotorPositionQuery{
					MotorPositionQuery{
						Mne:   "mne",
						Exact: []float64{1.23},
					},
				},
			},
		},
		{
			query: map[string]any{
				"motors.mne": map[string]any{"$in": []any{1.23, 4.56}},
			},
			expected: MotorsDbQuery{
				MotorPositionQueries: []MotorPositionQuery{
					MotorPositionQuery{
						Mne:   "mne",
						Exact: []float64{1.23, 4.56},
					},
				},
			},
		},
		{
			query: map[string]any{
				"motors": map[string]any{
					"mne": map[string]any{"$gt": -1.23, "$lt": 4.56, "$in": []any{0.0, 1.23}},
				},
			},
			expected: MotorsDbQuery{
				MotorPositionQueries: []MotorPositionQuery{
					MotorPositionQuery{
						Mne:   "mne",
						Exact: []float64{0, 1.23},
						Min:   -1.23,
						Max:   4.56,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			got := translateQuery(tt.query)
			if !equalMotorDbQuery(got, tt.expected) {
				t.Errorf("translateQuery(%v) = %v; want %v", tt.query, got, tt.expected)
			} else {
				log.Printf("Translated query: %+v; got %+v\n", tt.query, got)
			}
		})
	}
}

// equalMotorDbQuery checks if two MotorsDbQuery instances are identical.
func equalMotorDbQuery(a, b MotorsDbQuery) bool {
	// Compare Sids slices
	if len(a.Sids) != len(b.Sids) {
		return false
	}
	for i := range a.Sids {
		if a.Sids[i] != b.Sids[i] {
			return false
		}
	}
	// Sort MotorPositionQueries slices by the Mne field to ensure consistent ordering
	sort.Slice(a.MotorPositionQueries, func(i, j int) bool {
		return a.MotorPositionQueries[i].Mne < a.MotorPositionQueries[j].Mne
	})
	sort.Slice(b.MotorPositionQueries, func(i, j int) bool {
		return b.MotorPositionQueries[i].Mne < b.MotorPositionQueries[j].Mne
	})
	// Compare MotorPositionQueries slices
	if len(a.MotorPositionQueries) != len(b.MotorPositionQueries) {
		return false
	}
	for i := range a.MotorPositionQueries {
		if !equalMotorPositionQuery(a.MotorPositionQueries[i], b.MotorPositionQueries[i]) {
			return false
		}
	}
	return true
}

// equalMotorPositionQuery checks if two MotorPositionQuery instances are identical.
func equalMotorPositionQuery(a, b MotorPositionQuery) bool {
	// Compare Mne
	if a.Mne != b.Mne {
		return false
	}
	// Compare Exact slice
	if len(a.Exact) != len(b.Exact) {
		return false
	}
	for i := range a.Exact {
		if a.Exact[i] != b.Exact[i] {
			return false
		}
	}
	// Compare Min and Max
	return a.Min == b.Min && a.Max == b.Max
}
