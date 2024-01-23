INSERT INTO `pg-us-n-app-119329.dataobs.flipkart_mobile_prices`
SELECT
  trim(Brand),
  trim(MODEL),
  trim(Color),
  trim(Memory),
  trim(Storage),
  coalesce(Rating,0),
  coalesce(Selling_Price,0),
  coalesce(Original_Price,0)
FROM
  `pg-us-n-app-119329.dataobs.flipkart_mobile_prices`