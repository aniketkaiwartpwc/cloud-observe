CREATE OR REPLACE TABLE
  `pg-us-n-app-119329.dataobs.sales_transform_table` AS
SELECT
  Invoice_ID,
  Branch,
  City,
  Customer_type,
  Gender,
  Product_line,
  Total
FROM
  `pg-us-n-app-119329.dataobs.sales_table`
  LIMIT 1000;