INSERT INTO `pg-us-n-app-119329.dataobs.product_table`
SELECT
  coalesce(ProductID,0),
  trim(ProductName),
  trim(Category),
  coalesce(Price,0),
FROM
  `pg-us-n-app-119329.dataobs.product_table`