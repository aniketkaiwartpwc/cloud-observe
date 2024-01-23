CREATE OR REPLACE TABLE
  `pg-us-n-app-119329.dataobs.product_transform_table` AS
SELECT
  ProductID,
  ProductName,
  Category,
  Price
FROM
  `pg-us-n-app-119329.dataobs.product_table`
  LIMIT 1000;