-- Combines vendor telemetry and assigns a deployment Id to each telemetry record
WITH w_collar AS (
    SELECT *, upper(c.code_name) as vendor FROM collar 
    LEFT JOIN code c ON c.code_id = collar.device_make
  ), tel AS (
    SELECT idcollar as device_id, latitude, longitude, geom, acquisitiontime as acquisition_date, 'VECTRONIC' AS vendor, mainvoltage as mainbattvolt, temperature, idmortalitystatus = 5 AS mortality FROM telemetry_api_vectronic
    UNION ALL
    SELECT deviceid as device_id, latitude, longitude, geom, recdatetime as acquisition_date, 'LOTEK' AS vendor, mainv as mainbattvolt, temperature, null as mortality FROM telemetry_api_lotek
    UNION ALL
    SELECT collarserialnumber as device_id, latitude, longitude, geom, "date" as acquisition_date, 'ATS' AS vendor, battvoltage as mainbattvolt, temperature::int, mortality FROM telemetry_api_ats
  )
  select distinct tel.*, caa.deployment_id FROM collar_animal_assignment caa
  JOIN w_collar wc ON caa.collar_id = wc.collar_id
  LEFT JOIN tel ON tel.vendor = wc.vendor AND wc.device_id = tel.device_id 
  AND tel.acquisition_date >= caa.attachment_start
  AND tel.acquisition_date < caa.attachment_end;