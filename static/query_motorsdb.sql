SELECT S.sid, group_concat(M.motor_mne), group_concat(P.motor_position)
FROM MotorPositions AS P
JOIN MotorMnes AS M ON M.motor_id=P.motor_id
JOIN ScanIds AS S ON S.scan_id=M.scan_id
WHERE
{{ if gt (len .MotorPositionQueries) 0 }}

  S.scan_id IN (
    SELECT S.scan_id
    FROM MotorPositions AS P
    JOIN MotorMnes AS M ON M.motor_id=P.motor_id
    JOIN ScanIds AS S ON S.scan_id=M.scan_id
    WHERE

    {{ range $i, $MotorPositionQuery := .MotorPositionQueries }}
      {{ if $i }} OR {{ end }}
      M.motor_mne='{{ .Mne }}'

      {{ if or .Exact (or .Min .Max) }}
        AND (

        {{ if and .Min .Max }}
          P.motor_position BETWEEN {{ .Min }} AND {{ .Max }}
        {{ else if .Min }}
          P.motor_position>{{ .Min }}
        {{ else if .Max }}
          P.motor_position<{{ .Max }}
        {{ end }}

        {{ if and .Exact (or .Min .Max) }}
          OR
        {{ end }}

        {{ if .Exact }}
          {{ if eq 1 (len .Exact) }}
            P.motor_position={{ index .Exact 0 }}
          {{ else }}
            P.motor_position IN (
              {{ range $ii, $position := .Exact }}
                {{ if $ii }}, {{ end }}
                {{ $position }}
              {{ end }}
            )
          {{ end }}
        {{ end }}

        )
      {{ end }}

    {{ end }}
  )
  {{ if gt (len .Sids) 0 }}
    OR
  {{ end }}

{{ end }}

{{ if gt (len .Sids) 0 }}

  {{ if eq 1 (len .Sids) }}
    S.sid='{{ index .Sids 0 }}'
  {{ else }}
    S.sid IN (
      {{ range $i, $sid := .Sids }}
        {{ if $i }} , {{ end }}
        '{{ . }}'
      {{ end }}
    )
  {{ end }}

{{ end }}

GROUP BY S.sid;
