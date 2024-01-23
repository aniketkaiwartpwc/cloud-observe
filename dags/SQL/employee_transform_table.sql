CREATE OR REPLACE TABLE
  `pg-us-n-app-119329.dataobs.employee_transform_table` AS
SELECT
  Employee_ID,
  First_Name,
  Last_Name,
  Email,
  Age,
  City,
  State,
  Country,
  Occupation
FROM
  `pg-us-n-app-119329.dataobs.employee_table`
  LIMIT 1000;