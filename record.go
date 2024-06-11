package main

type Record struct {
	ScanId      uint64
	DatasetId   string             `json:"did"`
	Cycle       string             `json:"cycle"`
	Station     string             `json:"station"`
	Btr         string             `json:"btr"`
	SpecFile    string             `json:"spec_file"`
	ScanNumber  int                `json:"scan_number"`
	StartTime   float64            `json:"start_time"`
	Command     string             `json:"command"`
	Status      string             `json:"status"`
	Comments    []string           `json:"comments"`
	SpecVersion string             `json:"spec_version"`
	Motors      map[string]float64 `json:"motors"`
}

func GetRecords(record map[string]any) (Record, MotorRecord, error) {
	// Decompose a user-submitted record into two pieces:
	// the mongodb portion (no motor positions), and
	// the sql portion (only motor positions).
	// Get the record's dataset ID and include it in both records.
	var mongo_record Record
	var motor_record MotorRecord
	return mongo_record, motor_record, nil
}

func CompleteRecord(record map[string]any, motor_record MotorRecord) map[string]any {
	record["Motors"] = motor_record.Motors
	return record
}
