CREATE OR REPLACE TABLE
  `pg-us-n-app-119329.dataobs.gender_transform_table` AS
SELECT
  customerID,
  Weight,
  Height,
  Age,
  Gender
FROM
  `pg-us-n-app-119329.dataobs.gender_table`
  LIMIT 1000;