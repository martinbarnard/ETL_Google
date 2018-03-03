#!/usr/bin/env python3

# From the API ref docs: https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python
from google.cloud import bigquery
import sql

def create_dataset(ds_id=None):
    '''
    Will create a shiny new dataset & return a dataset object
    TODO: If we were concerned with stupidity (i.e. non savvy users), we would be validating
    '''
    bigquery_client = bigquery.Client()

    dataset_ref = bigquery_client.dataset(ds_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset = bigquery_client.create_dataset(dataset)

    # This will be logging shortly
    print('Dataset {} created.'.format(dataset.dataset_id))

    return dataset


def run_qry(query, params):
    '''
    Wrapper around our query runner: Will allow us to validate inputs. We can also limit the queries to pre-created & 
    validated ones
    '''

    return dict()


def main():
    '''
    Assumes our creds are somewhere!!!
    '''
    print('pulling in most snow query')
    # Most snow query is pulling days / name / country
    qry = sql.qry_most_snow()
    cl = bigquery.Client()
    job=cl.query(qry)
    print('Query running')
    rows = job.result()
    for row in rows:
        print("{} - {} ({})".format(row.days, row.name, row.country))

    print('all done')
        



if __name__=='__main__':
    main()



