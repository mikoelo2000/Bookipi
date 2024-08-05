CREATE OR REPLACE PROCEDURE `BookipiProject.BookipiProduction.TransferAndDeduplicate_Subscription_Payments`()
BEGIN
  -- Create a staging table with cleaned and validated data
  CREATE OR REPLACE TABLE `BookipiProject.BookipiProduction.Subscription_Payments_temp` AS
  SELECT
    Subscription_Payments_ID,
    Subscription_ID,
    date,
    billingPeriod,
    amount,
    tax,
    totalExcludingTax,
    start_date,
    end_date,
    extraction_date
  FROM `BookipiProject.BookipiProduction.Subscription_Payments_Transformed`;

  -- Remove duplicates based on Subscription_Payments_ID and keep the most recent record
  CREATE OR REPLACE TABLE `BookipiProject.BookipiProduction.Subscription_Payments_cleaned` AS
  SELECT
    * EXCEPT (row_number)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY Subscription_Payments_ID ORDER BY extraction_date DESC) AS row_number
    FROM `BookipiProject.BookipiProduction.Subscription_Payments_temp`
  )
  WHERE row_number = 1;

  -- Replace the old production table with the cleaned table
  CREATE OR REPLACE TABLE `BookipiProject.BookipiProduction.Subscription_Payments` AS
  SELECT * FROM `BookipiProject.BookipiProduction.Subscription_Payments_cleaned`;

  -- Drop the temporary tables used in the process
  DROP TABLE `BookipiProject.BookipiProduction.Subscription_Payments_temp`;
  DROP TABLE `BookipiProject.BookipiProduction.Subscription_Payments_cleaned`;
END;
