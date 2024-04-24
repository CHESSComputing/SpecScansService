SELECT D.did, group_concat(M.motor_mne), group_concat(P.motor_position)
FROM MotorPositions as P
JOIN MotorMnes AS M ON M.motor_id=P.motor_id
JOIN DID AS D ON D.dataset_id=M.dataset_id
WHERE
{{ if gt (len .MotorPositionQueries) 0 }}

  D.dataset_id IN (
    SELECT D.dataset_id
    FROM MotorPositions as P
    JOIN MotorMnes AS M ON M.motor_id=P.motor_id
    JOIN DID AS D ON D.dataset_id=M.dataset_id
    WHERE

    {{ range .MotorPositionQueries }}
      M.motor_mne='{{ .Mne }}'

      {{ if or .Exact (or .Min .Max) }}
        AND (

        {{ if and .Min .Max }}
          P.motor_position BETWEEN {{ .Min }} AND {{ .Max }}
        {{ else if .Min }}
          P.motor_position>{{ .Min }}
        {{ else if .Max }}
          P.motor_position<{{ .Min }}
        {{ end }}

        {{ if and .Exact (or .Min .Max) }}
          OR
        {{ end }}

        {{ if .Exact }}
          {{ if eq 1 (len .Exact) }}
            P.motor_position={{ index .Exact 0 }}
          {{ else }}
            P.motor_position IN (
              {{ range .Exact }}
                {{ . }},
              {{ end }}
            )
          {{ end }}
        {{ end }}

        )
      {{ end }}

    {{ end }}
  )
  {{ if gt (len .Dids) 0 }}
    OR
  {{ end }}

{{ end }}

{{ if gt (len .Dids) 0 }}

  {{ if eq 1 (len .Dids) }}
    D.did='{{ index .Dids 0 }}'
  {{ else }}
    D.did IN (
      {{ range .Dids }}
        '{{ . }}',
      {{ end }}
    )
  {{ end }}

{{ end }}

GROUP BY D.did;
