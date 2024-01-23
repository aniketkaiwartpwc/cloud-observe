INSERT INTO `pg-us-n-app-119329.dataobs.customer_table`
SELECT
  coalesce(CustomerID,0),
  trim(Gender),
  trim(Location),
  coalesce(Tenure_Months,0),
  coalesce(Transaction_ID,0),
  trim(Transaction_Date),
  trim(Product_SKU)
FROM
  `pg-us-n-app-119329.dataobs.customer_table`
