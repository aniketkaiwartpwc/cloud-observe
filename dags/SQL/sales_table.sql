INSERT INTO `pg-us-n-app-119329.dataobs.sales_table`
SELECT
  coalesce(Invoice_ID,0),
  trim(Branch),
  trim(City),
  trim(Customer_type),
  trim(Gender),
  trim(Product_line),
  coalesce(Total,0)
FROM
  `pg-us-n-app-119329.dataobs.sales_table`