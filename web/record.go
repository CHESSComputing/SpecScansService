package main

type Record struct {
	SpecFile       string    `json:"specfile"`
	ScanNumber     int       `json:"scannumber"`
	Command        string    `json:"command"`
	MotorMnes      []string  `json:"motor_mnes"`
	MotorPositions []float64 `json:"motor_positions"`
}
