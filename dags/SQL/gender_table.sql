INSERT INTO `pg-us-n-app-119329.dataobs.gender_table`
SELECT
  coalesce(customerID,0),
  coalesce(Weight,0),
  coalesce(Height,0),
  coalesce(Age,0),
  trim(Gender)
FROM
  `pg-us-n-app-119329.dataobs.gender_table`