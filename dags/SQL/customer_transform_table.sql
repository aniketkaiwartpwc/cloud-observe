CREATE OR REPLACE TABLE
  `pg-us-n-app-119329.dataobs.customer_transform_table` AS
SELECT
  CustomerID,
  Gender,
  Location,
  Tenure_Months,
  Transaction_ID,
  Transaction_Date,
  Product_SKU
FROM
  `pg-us-n-app-119329.dataobs.customer_table`
  LIMIT 1000;