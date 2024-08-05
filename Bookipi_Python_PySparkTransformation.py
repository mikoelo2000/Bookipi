from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, to_date

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TransformSubscriptionPayments") \
    .getOrCreate()

# Load data from BigQuery
df = spark.read.format('bigquery') \
    .option('table', 'BookipiProject.BookipiProduction.Subscription_Payments') \
    .load()

# Split the billingPeriod into start and end dates
df = df.withColumn('start_date', split(col('billingPeriod'), ' - ').getItem(0))
df = df.withColumn('end_date', split(col('billingPeriod'), ' - ').getItem(1))

# Convert the extracted start_date and end_date to DATE type
df = df.withColumn('start_date', to_date(col('start_date'), 'yyyyMMdd'))
df = df.withColumn('end_date', to_date(col('end_date'), 'yyyyMMdd'))

# Show the transformed DataFrame (for debugging purposes)
df.show()

# Save the transformed DataFrame back to a new BigQuery staging table
df.write.format('bigquery') \
    .option('table', 'BookipiProject.BookipiProduction.Subscription_Payments_Transformed') \
    .mode('overwrite') \
    .save()
