CREATE OR REPLACE PROCEDURE `BookipiProject.BookipiSample.HandleDuplicates_User`()
BEGIN
  -- Create or replace the staging table with deduplicated data
  CREATE OR REPLACE TABLE `BookipiProject.BookipiStaging.user_staging` AS
  SELECT
    User_ID,
    email,
    `default_company`,
    extraction_date
  FROM (
    SELECT
      User_ID,
      email,
      `default_company`,
      extraction_date,
      ROW_NUMBER() OVER (PARTITION BY User_ID ORDER BY extraction_date DESC) AS rn
    FROM `BookipiProject.BookipiSample.user_table`
  )
  WHERE rn = 1;

END;

CREATE OR REPLACE PROCEDURE `BookipiProject.BookipiSample.HandleDuplicates_Company`()
BEGIN
  -- Create or replace the staging table with deduplicated data
  CREATE OR REPLACE TABLE `BookipiProject.BookipiStaging.company_staging` AS
  SELECT
    Company_ID,
    owner,
    company_name,
    country,
    state,
    currency,
    email,
    extraction_date
  FROM (
    SELECT
      Company_ID,
      owner,
      company_name,
      country,
      state,
      currency,
      email,
      extraction_date,
      ROW_NUMBER() OVER (PARTITION BY Company_ID ORDER BY extraction_date DESC) AS rn
    FROM `BookipiProject.BookipiSample.company_table`
  )
  WHERE rn = 1;

END;

CREATE OR REPLACE PROCEDURE `BookipiProject.BookipiSample.HandleDuplicates_Subscription`()
BEGIN
  -- Create or replace the staging table with deduplicated data
  CREATE OR REPLACE TABLE `BookipiProject.BookipiStaging.subscription_staging` AS
  SELECT
    Subscription_ID,
    Company_ID,
    status,
    subType AS subscription_type,
    billingPeriod AS billing_period,
    startDate AS start_date,
    endDate AS end_date,
    nextBillingDate AS next_billing_date,
    extraction_date
  FROM (
    SELECT
      Subscription_ID,
      Company_ID,
      status,
      subType AS subscription_type,
      billingPeriod AS billing_period,
      startDate AS start_date,
      endDate AS end_date,
      nextBillingDate AS next_billing_date,
      extraction_date,
      ROW_NUMBER() OVER (PARTITION BY Subscription_ID ORDER BY extraction_date DESC) AS rn
    FROM `BookipiProject.BookipiSample.subscription_table`
  )
  WHERE rn = 1;

END;

CREATE OR REPLACE PROCEDURE `BookipiProject.BookipiSample.HandleDuplicates_Subscription_Payments`()
BEGIN
  -- Create or replace the staging table with deduplicated data
  CREATE OR REPLACE TABLE `BookipiProject.BookipiStaging.subscription_payments_staging` AS
  SELECT
    Subscription_Payments_ID,
    Subscription_ID,
    date,
    billingPeriod AS billing_period,
    amount,
    tax,
    totalExcludingTax AS total_excluding_tax,
    status,
    extraction_date
  FROM (
    SELECT
      Subscription_Payments_ID,
      Subscription_ID,
      date,
      billingPeriod AS billing_period,
      amount,
      tax,
      totalExcludingTax AS total_excluding_tax,
      status,
      extraction_date,
      ROW_NUMBER() OVER (PARTITION BY Subscription_Payments_ID ORDER BY extraction_date DESC) AS rn
    FROM `BookipiProject.BookipiSample.subscription_payments_table`
  )
  WHERE rn = 1;

END;

-- Define the stored procedure to handle duplicates in the Invoice table
CREATE OR REPLACE PROCEDURE `BookipiProject.BookipiStaging.HandleDuplicates_Invoice`()
BEGIN
  -- Create a temporary table to hold unique records
  CREATE OR REPLACE TABLE `BookipiProject.BookipiStaging.Temp_Invoice` AS
  SELECT
    *
  FROM (
    SELECT
      Invoice_ID,
      Company_ID,
      list_of_items,
      list_of_payments,
      ROW_NUMBER() OVER (PARTITION BY Invoice_ID ORDER BY extraction_date DESC) AS row_num
    FROM `BookipiProject.BookipiSample.Invoice`
  )
  WHERE row_num = 1;

  -- Delete all records from the original table
  DELETE FROM `BookipiProject.BookipiStaging.Invoice` WHERE TRUE;

  -- Insert unique records back into the original table
  INSERT INTO `BookipiProject.BookipiStaging.Invoice` (
    Invoice_ID,
    Company_ID,
    list_of_items,
    list_of_payments
  )
  SELECT
    Invoice_ID,
    Company_ID,
    list_of_items,
    list_of_payments
  FROM `BookipiProject.BookipiStaging.Temp_Invoice`;

  -- Drop the temporary table
  DROP TABLE `BookipiProject.BookipiStaging.Temp_Invoice`;
END;

-- Create the Items table in BookipiStaging dataset
CREATE TABLE IF NOT EXISTS `BookipiProject.BookipiStaging.Items` (
  Invoice_ID STRING,
  Item_Name STRING,
  Quantity INT64,
  Price FLOAT64
);

-- Create the Payments table in BookipiStaging dataset
CREATE TABLE IF NOT EXISTS `BookipiProject.BookipiStaging.Payments` (
  Invoice_ID STRING,
  Payment_Amount FLOAT64,
  Payment_Date DATE
);


-- Define the stored procedure to append list_of_items to the Items table in staging
CREATE OR REPLACE PROCEDURE `BookipiProject.BookipiStaging.AppendItemsToItems`()
BEGIN
  -- Insert flattened list_of_items into Items table
  INSERT INTO `BookipiProject.BookipiStaging.Items` (Invoice_ID, Item_Name, Quantity, Price)
  SELECT
    Invoice_ID,
    item.name AS Item_Name,
    item.quantity AS Quantity,
    item.price AS Price
  FROM `BookipiProject.BookipiSample.Invoice`,
  UNNEST(list_of_items) AS item;
END;

-- Define the stored procedure to append list_of_payments to the Payments table in staging
CREATE OR REPLACE PROCEDURE `BookipiProject.BookipiStaging.AppendPaymentsToPayments`()
BEGIN
  -- Insert flattened list_of_payments into Payments table
  INSERT INTO `BookipiProject.BookipiStaging.Payments` (Invoice_ID, Payment_Amount, Payment_Date)
  SELECT
    Invoice_ID,
    payment.amount AS Payment_Amount,
    payment.date AS Payment_Date
  FROM `BookipiProject.BookipiSample.Invoice`,
  UNNEST(list_of_payments) AS payment;
END;

