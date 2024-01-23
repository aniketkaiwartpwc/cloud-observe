INSERT INTO `pg-us-n-app-119329.dataobs.employee_table`
SELECT
  coalesce(Employee_ID,0),
  trim(First_Name),
  trim(Last_Name),
  trim(Email),
  coalesce(Age,0),
  trim(City),
  trim(State),
  trim(Country),
  trim(Occupation)
FROM
  `pg-us-n-app-119329.dataobs.employee_table`