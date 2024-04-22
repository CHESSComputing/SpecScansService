package main

type Record struct {
	SpecFile       string    `json:"specfile"`
	ScanNumber     int       `json:"scannumber"`
	Command        string    `json:"command"`
	MotorMnes      []string  `json:"motor_mnes"`
	MotorPositions []float64 `json:"motor_positions"`
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
