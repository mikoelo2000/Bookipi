import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from datetime import datetime
import pymongo
import pandas as pd
from google.cloud import bigquery

class ExtractFromMongoDB(beam.DoFn):
    def __init__(self, collection_name):
        self.collection_name = collection_name

    def process(self, element):
        client = pymongo.MongoClient('mongodb://username:password@host:port/dbname')
        db = client['your_database']
        collection = db[self.collection_name]
        cursor = collection.find()
        for document in cursor:
            yield document

class TransformData(beam.DoFn):
    def process(self, element):
        # Add extraction date
        extraction_date = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        
        # Rename shorthand notation to full names
        rename_dict = {
            'dci': 'default_company',
            'c': 'company_name',
            'co': 'country',
            'st': 'state',
            'cr': 'currency',
            'ui': 'company_id',
            'it': 'list_of_items',
            'pm': 'list_of_payments',
            'subType': 'subscription_type',
            'billingPeriod': 'billing_period',
            'startDate': 'start_date',
            'endDate': 'end_date',
            'nextBillingDate': 'next_billing_date',
            'totalExcludingTax': 'total_excluding_tax'
        }
        
        # Rename columns based on the dictionary
        element = {rename_dict.get(k, k): v for k, v in element.items()}
        
        # Add extraction date column
        element['extraction_date'] = extraction_date
        
        # Replace NaNs with empty strings
        if 'list_of_items' in element:
            element['list_of_items'] = element.get('list_of_items', [])
        
        yield element

def run():
    pipeline_options = PipelineOptions()
    
    p = beam.Pipeline(options=pipeline_options)

    # List of collections and their corresponding BigQuery tables
    collections = {
        'User': 'BookipiProject:BookipiSample.user_table',
        'Company': 'BookipiProject:BookipiSample.company_table',
        'Subscription': 'BookipiProject:BookipiSample.subscription_table',
        'Invoice': 'BookipiProject:BookipiSample.invoice_table',
        'Subscription_Payments': 'BookipiProject:BookipiSample.subscription_payments_table'
    }

    for collection_name, table_name in collections.items():
        # Handle each collection
        elements = (
            p
            | f'ReadFromMongo_{collection_name}' >> beam.ParDo(ExtractFromMongoDB(collection_name))
            | f'TransformData_{collection_name}' >> beam.ParDo(TransformData())
        )
        
        (elements
         | f'WriteToBigQuery_{collection_name}' >> beam.io.WriteToBigQuery(
             table_name,
             schema='SCHEMA_AUTODETECT',  # Or provide schema manually
             write_disposition=BigQueryDisposition.WRITE_APPEND
         )
        )

    result = p.run()
    result.wait_until_finish()

    # Call BigQuery stored procedures after pipeline completes
    client = bigquery.Client()
    dataset_id = 'BookipiSample'
    staging_dataset_id = 'BookipiStaging'
    project_id = 'BookipiProject'
    production_dataset_id = 'BookipiProduction'

     # Stored procedures for staging dataset
    staging_procedures = [
        f'CALL `{project_id}.{staging_dataset_id}.HandleDuplicates_User`();',
        f'CALL `{project_id}.{staging_dataset_id}.HandleDuplicates_Company`();',
        f'CALL `{project_id}.{staging_dataset_id}.HandleDuplicates_Subscription`();',
        f'CALL `{project_id}.{staging_dataset_id}.HandleDuplicates_Invoice`();',
        f'CALL `{project_id}.{staging_dataset_id}.HandleDuplicates_Subscription_Payments`();
        f'CALL `{project_id}.{staging_dataset_id}.AppendItemsToItems`();',
        f'CALL `{project_id}.{staging_dataset_id}.AppendPaymentsToPayments`();'
    ]
    
    # Execute staging procedures
    for procedure in staging_procedures:
        query_job = client.query(procedure)
        query_job.result()  # Wait for the job to complete

    # Stored procedure for production dataset
    production_procedures = [
        f'CALL `{project_id}.{production_dataset_id}.HandleDuplicates_User`();',
        f'CALL `{project_id}.{production_dataset_id}.HandleDuplicates_Company`();',
        f'CALL `{project_id}.{production_dataset_id}.HandleDuplicates_Subscription`();',
        f'CALL `{project_id}.{production_dataset_id}.HandleDuplicates_Invoice`();',
        f'CALL `{project_id}.{production_dataset_id}.HandleDuplicates_Subscription_Payments`();'
    ]

    # Execute production procedures
    for procedure in production_procedures:
        query_job = client.query(procedure)
        query_job.result()  # Wait for the job to complete

if __name__ == '__main__':
    run()
