INSERT INTO `pg-us-n-app-119329.dataobs.customer_table`
SELECT
  coalesce(CustomerID,0),
  trim(Gender),
  trim(Location),
  coalesce(Tenure_Months,0),
  coalesce(Transaction_ID,0),
  trim(Transaction_Date),
  trim(Product_SKU),
  trim(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', Job_Run_Date))
FROM
  `pg-us-n-app-119329.dataobs.customer_table`