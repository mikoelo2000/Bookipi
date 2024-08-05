CREATE OR REPLACE PROCEDURE `BookipiProject.BookipiProduction.CreateWeeklyChurnReport`()
BEGIN
  -- Create or replace the report table with new weekly data
  CREATE OR REPLACE TABLE `BookipiProject.BookipiProduction.Weekly_Churn_Analysis` AS
  WITH subscription_data AS (
    SELECT
      EXTRACT(YEAR FROM startDate) AS year,
      EXTRACT(WEEK FROM startDate) AS week,
      COUNTIF(status = 'active') AS new_subscriptions,
      COUNTIF(status = 'cancelled') AS cancelled_subscriptions,
      COUNTIF(status = 'terminated') AS terminated_subscriptions
    FROM `BookipiProject.BookipiProduction.Subscription`
    WHERE startDate IS NOT NULL
    GROUP BY year, week
  ),
  weekly_summary AS (
    SELECT
      year,
      week,
      SUM(new_subscriptions) AS total_new_subscriptions,
      SUM(cancelled_subscriptions) AS total_cancelled_subscriptions,
      SUM(terminated_subscriptions) AS total_terminated_subscriptions
    FROM subscription_data
    GROUP BY year, week
  )
  SELECT
    year,
    week,
    total_new_subscriptions,
    total_cancelled_subscriptions,
    total_terminated_subscriptions
  FROM weekly_summary
  ORDER BY year DESC, week DESC;
END;
