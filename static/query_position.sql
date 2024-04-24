SELECT D.did, group_concat(M.motor_mne), group_concat(P.motor_position)
FROM MotorPositions as P
JOIN MotorMnes AS M ON M.motor_id=P.motor_id
JOIN DID AS D ON D.dataset_id=M.dataset_id
WHERE D.dataset_id IN (
  SELECT D.dataset_id
  FROM MotorPositions as P
  JOIN MotorMnes AS M ON M.motor_id=P.motor_id
  JOIN DID AS D ON D.dataset_id=M.dataset_id
  WHERE M.motor_mne='{{ .MotorMne }}' AND P.motor_position={{ .MotorPosition }})
GROUP BY D.did;
