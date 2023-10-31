package main

type Record struct {
	SpecFile   string `json:"specfile"`
	ScanNumber int    `json:"scannumber"`
	Command    string `json:"command"`
}
