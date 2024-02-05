INSERT INTO `pg-us-n-app-119329.dataobs.gender_table`
SELECT
  coalesce(customerID,0),
  coalesce(Weight,0),
  coalesce(Height,0),
  coalesce(Age,0),
  trim(Gender),
  TRIM(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', Job_Run_Date))
FROM
  `pg-us-n-app-119329.dataobs.gender_table`