CREATE TABLE IF NOT EXISTS ScanIds (
scan_id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
sid VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS MotorMnes (
motor_id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
motor_mne VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS MotorPositions (
scan_id INTEGER NOT NULL,
motor_id INTEGER NOT NULL,
motor_position DOUBLE,
FOREIGN KEY (motor_id) REFERENCES MotorMnes(motor_id) ON DELETE CASCADE ON UPDATE ,
FOREIGN KEY (scan_id) REFERENCES ScanIds(scan_id) ON DELETE CASCADE ON UPDATE CASCADE
);


-- Individual Indexes
CREATE INDEX idx_sid ON ScanIds(sid);
CREATE INDEX idx_motor_mne ON MotorMnes(motor_mne);
CREATE INDEX idx_motor_position ON MotorPositions(motor_position);

-- Multi-column index on (sid, motor_mne, motor_position)
CREATE INDEX idx_sid_motor_mne_position
ON MotorPositions(motor_id, motor_position);
