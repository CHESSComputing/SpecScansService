SELECT D.did, group_concat(M.motor_mne), group_concat(P.motor_position)
FROM MotorPositions as P
JOIN MotorMnes AS M ON M.motor_id=P.motor_id
JOIN DID AS D ON D.dataset_id=M.dataset_id
WHERE D.did='{{.DatasetId}}'
GROUP BY D.did;
