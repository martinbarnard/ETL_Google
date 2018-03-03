#!/usr/bin/env python3

# From the API ref docs: https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python
from google.cloud import bigquery
bigquery_client = bigquery.Client()
dataset_id = 'bob_da_builder'
dataset_ref = bigquery_client.dataset(dataset_id)
dataset = bigquery.Dataset(dataset_ref)
dataset = bigquery_client.create_dataset(dataset)

print('Dataset {} created.'.format(dataset.dataset_id))




# SQL EXAMPLES!!!
#### SELECT
####   CONCAT(
####     'https://stackoverflow.com/questions/',
####     CAST(id as STRING)) as url,
####   view_count
#### FROM `bigquery-public-data.stackoverflow.posts_questions`
#### WHERE tags like '%google-bigquery%'
#### ORDER BY view_count DESC
#### LIMIT 10


