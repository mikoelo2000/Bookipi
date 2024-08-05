CREATE OR REPLACE PROCEDURE `BookipiProject.BookipiProduction.HandleDuplicates_User()
BEGIN
  -- Step 1: Create the new User table in production if it does not exist
  CREATE TABLE IF NOT EXISTS `BookipiProject.BookipiProduction.User` (
    User_ID STRING,
    email STRING,
    default_company STRING,
    extraction_date TIMESTAMP
  );

  -- Step 2: Insert new data from the staging table into the production table
  -- This will overwrite the existing data in the production table with new data
  CREATE OR REPLACE TABLE `BookipiProject.BookipiProduction.User_temp` AS
  SELECT
    *
  FROM `BookipiProject.BookipiStaging.User`;

  -- Step 3: Remove duplicates based on User_ID and keep the most recent record
  CREATE OR REPLACE TABLE `BookipiProject.BookipiProduction.User_cleaned` AS
  SELECT
    * EXCEPT (row_number)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY User_ID ORDER BY extraction_date DESC) AS row_number
    FROM `BookipiProject.BookipiProduction.User_temp`
  )
  WHERE row_number = 1;

  -- Step 4: Replace the old production table with the cleaned table
  CREATE OR REPLACE TABLE `BookipiProject.BookipiProduction.User` AS
  SELECT * FROM `BookipiProject.BookipiProduction.User_cleaned`;

  -- Drop the temporary tables used in the process
  DROP TABLE `BookipiProject.BookipiProduction.User_temp`;
  DROP TABLE `BookipiProject.BookipiProduction.User_cleaned`;
END;

CREATE OR REPLACE PROCEDURE `BookipiProject.BookipiProduction.HandleDuplicates_Company`()
BEGIN
  -- Create the new Company table in production if it does not exist
  CREATE TABLE IF NOT EXISTS `BookipiProject.BookipiProduction.Company` (
    Company_ID STRING,
    owner STRING,
    company_name STRING,
    country STRING,
    state STRING,
    currency STRING,
    email STRING,
    extraction_date TIMESTAMP
  );

  -- Insert new data from the staging table into the production table
  CREATE OR REPLACE TABLE `BookipiProject.BookipiProduction.Company_temp` AS
  SELECT
    *
  FROM `BookipiProject.BookipiStaging.Company`;

  -- Remove duplicates based on Company_ID and keep the most recent record
  CREATE OR REPLACE TABLE `BookipiProject.BookipiProduction.Company_cleaned` AS
  SELECT
    * EXCEPT (row_number)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY Company_ID ORDER BY extraction_date DESC) AS row_number
    FROM `BookipiProject.BookipiProduction.Company_temp`
  )
  WHERE row_number = 1;

  -- Replace the old production table with the cleaned table
  CREATE OR REPLACE TABLE `BookipiProject.BookipiProduction.Company` AS
  SELECT * FROM `BookipiProject.BookipiProduction.Company_cleaned`;

  -- Drop the temporary tables used in the process
  DROP TABLE `BookipiProject.BookipiProduction.Company_temp`;
  DROP TABLE `BookipiProject.BookipiProduction.Company_cleaned`;
END;

CREATE OR REPLACE PROCEDURE `BookipiProject.BookipiProduction.HandleDuplicates_Subscription`()
BEGIN
 -- Create a staging table with cleaned and validated data
  CREATE OR REPLACE TABLE `BookipiProject.BookipiProduction.Subscription_temp` AS
  SELECT
    Subscription_ID,
    Company_ID,
    -- Validate and clean status
    CASE
      WHEN status IN ('active', 'cancelled', 'terminated') THEN status
      ELSE NULL
    END AS status,
    -- Validate and clean subType
    CASE
      WHEN subType IN ('started', 'pro') THEN subType
      ELSE NULL
    END AS subType,
    -- Validate and clean billingPeriod
    CASE
      WHEN billingPeriod IN ('month', 'annual') THEN billingPeriod
      ELSE NULL
    END AS billingPeriod,
    -- Convert startDate and endDate to DATE format
    PARSE_DATE('%Y-%m-%d', FORMAT_TIMESTAMP('%Y-%m-%d', startDate)) AS startDate,
    PARSE_DATE('%Y-%m-%d', FORMAT_TIMESTAMP('%Y-%m-%d', endDate)) AS endDate,
    nextBillingDate,
    extraction_date
  FROM `BookipiProject.BookipiStaging.Subscription`;

  -- Remove duplicates based on Subscription_ID and keep the most recent record
  CREATE OR REPLACE TABLE `BookipiProject.BookipiProduction.Subscription_cleaned` AS
  SELECT
    * EXCEPT (row_number)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY Subscription_ID ORDER BY extraction_date DESC) AS row_number
    FROM `BookipiProject.BookipiProduction.Subscription_temp`
  )
  WHERE row_number = 1;

  -- Replace the old production table with the cleaned table
  CREATE OR REPLACE TABLE `BookipiProject.BookipiProduction.Subscription` AS
  SELECT * FROM `BookipiProject.BookipiProduction.Subscription_cleaned`;

  -- Drop the temporary tables used in the process
  DROP TABLE `BookipiProject.BookipiProduction.Subscription_temp`;
  DROP TABLE `BookipiProject.BookipiProduction.Subscription_cleaned`;
END;

CREATE OR REPLACE PROCEDURE `BookipiProject.BookipiProduction.HandleDuplicates_Invoice`()
BEGIN
  -- Create the new Invoice table in production if it does not exist
  CREATE TABLE IF NOT EXISTS `BookipiProject.BookipiProduction.Invoice` (
    Invoice_ID STRING,
    Company_ID STRING,
    list_of_items ARRAY<STRUCT<name STRING, quantity INT64, price FLOAT64>>,
    list_of_payments ARRAY<STRUCT<amount FLOAT64, date TIMESTAMP>>,
    extraction_date TIMESTAMP
  );

  -- Insert new data from the staging table into the production table
  CREATE OR REPLACE TABLE `BookipiProject.BookipiProduction.Invoice_temp` AS
  SELECT
    *
  FROM `BookipiProject.BookipiStaging.Invoice`;

  -- Remove duplicates based on Invoice_ID and keep the most recent record
  CREATE OR REPLACE TABLE `BookipiProject.BookipiProduction.Invoice_cleaned` AS
  SELECT
    * EXCEPT (row_number)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY Invoice_ID ORDER BY extraction_date DESC) AS row_number
    FROM `BookipiProject.BookipiProduction.Invoice_temp`
  )
  WHERE row_number = 1;

  -- Replace the old production table with the cleaned table
  CREATE OR REPLACE TABLE `BookipiProject.BookipiProduction.Invoice` AS
  SELECT * FROM `BookipiProject.BookipiProduction.Invoice_cleaned`;

  -- Drop the temporary tables used in the process
  DROP TABLE `BookipiProject.BookipiProduction.Invoice_temp`;
  DROP TABLE `BookipiProject.BookipiProduction.Invoice_cleaned`;
END;

CREATE OR REPLACE PROCEDURE `BookipiProject.BookipiProduction.HandleDuplicates_Invoice`()
BEGIN
  -- Create the new Invoice table in production if it does not exist
  CREATE TABLE IF NOT EXISTS `BookipiProject.BookipiProduction.Invoice` (
    Invoice_ID STRING,
    Company_ID STRING,
    list_of_items ARRAY<STRUCT<name STRING, quantity INT64, price FLOAT64>>,
    list_of_payments ARRAY<STRUCT<amount FLOAT64, date TIMESTAMP>>,
    extraction_date TIMESTAMP
  );

  -- Insert new data from the staging table into the production table
  CREATE OR REPLACE TABLE `BookipiProject.BookipiProduction.Invoice_temp` AS
  SELECT
    *
  FROM `BookipiProject.BookipiStaging.Invoice`;

  -- Remove duplicates based on Invoice_ID and keep the most recent record
  CREATE OR REPLACE TABLE `BookipiProject.BookipiProduction.Invoice_cleaned` AS
  SELECT
    * EXCEPT (row_number)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY Invoice_ID ORDER BY extraction_date DESC) AS row_number
    FROM `BookipiProject.BookipiProduction.Invoice_temp`
  )
  WHERE row_number = 1;

  -- Replace the old production table with the cleaned table
  CREATE OR REPLACE TABLE `BookipiProject.BookipiProduction.Invoice` AS
  SELECT * FROM `BookipiProject.BookipiProduction.Invoice_cleaned`;

  -- Drop the temporary tables used in the process
  DROP TABLE `BookipiProject.BookipiProduction.Invoice_temp`;
  DROP TABLE `BookipiProject.BookipiProduction.Invoice_cleaned`;
END;

CREATE OR REPLACE PROCEDURE `BookipiProject.BookipiProduction.HandleDuplicates_Payments`()
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
    -- Validate and clean status
    CASE
      WHEN status IN ('pending', 'success', 'failed') THEN status
      ELSE NULL
    END AS status,
    extraction_date
  FROM `BookipiProject.BookipiStaging.Subscription_Payments`;

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

CREATE OR REPLACE PROCEDURE `BookipiProject.BookipiProduction.HandleDuplicates_Items`()
BEGIN
  -- Create the new Items table in production if it does not exist
  CREATE TABLE IF NOT EXISTS `BookipiProject.BookipiProduction.Items` (
    Item_ID STRING,  -- Assuming Item_ID as the primary key
    name STRING,
    quantity INT64,
    price FLOAT64,
    extraction_date TIMESTAMP
  );

  -- Insert new data from the staging table into the production table
  CREATE OR REPLACE TABLE `BookipiProject.BookipiProduction.Items_temp` AS
  SELECT
    item.name AS name,
    item.quantity AS quantity,
    item.price AS price,
    i.extraction_date
  FROM `BookipiProject.BookipiStaging.Invoice` AS i,
  UNNEST(i.list_of_items) AS item;

  -- Remove duplicates based on a combination of Item_ID or relevant attributes
  -- Here assuming Item_ID is an identifier within the list_of_items array
  CREATE OR REPLACE TABLE `BookipiProject.BookipiProduction.Items_cleaned`

END;
