from google.cloud import bigquery
import tables
import time
import yaml
import sys
import json
import os
import datetime

class BigQueryClient:
    def __init__(self, project_name, input_bucket, input_folder, dataset_name):
        self.bigquery_client = bigquery.Client()
        self.project_name = project_name
        self.input_bucket = input_bucket
        self.input_folder = input_folder
        self.dataset_name = dataset_name
        self.dataset = project_name + '.' + dataset_name
        self.mapping = {
                'lineitem' : tables.lineitem_schema,
                'customer' : tables.customer_schema,
                'supplier' : tables.supplier_schema,
                'nation' : tables.nation_schema,
                'region' : tables.region_schema,
                'partsupp' : tables.partsupp_schema,
                'orders' : tables.orders_schema,
                'part' : tables.part_schema
                }

    def create_dataset(self):
        dataset = bigquery.Dataset(self.dataset)
        dataset.location = 'europe-west6'
        dataset = self.bigquery_client.create_dataset(dataset, timeout=30)
        print("Created dataset {}.{}".format(self.bigquery_client.project,
                                             dataset.dataset_id))

    def create_table(self, table_name, data_format):
        dataset_ref = bigquery.DatasetReference(self.project_name,
                                                self.dataset_name)
        table_id = table_name
        uri = "gs://" + str(self.input_bucket) + "/" + \
                    str(self.input_folder) + "/" + table_name + \
                    "/" + table_name + "*"

        table = bigquery.Table(dataset_ref.table(table_id),
                               schema=self.mapping[table_name])

        if data_format == 'csv':
            external_config = bigquery.ExternalConfig("CSV")
            external_config.options.field_delimiter = "|"
        elif data_format == 'parquet':
            external_config = bigquery.ExternalConfig("PARQUET")

        external_config.source_uris = [uri]
        table.external_data_configuration = external_config
        table = self.bigquery_client.create_table(table)

        #destination_table = self.bigquery_client.get_table(table_id)
        #print("Table {} has {} rows.".format(destination_table,
        #                                     destination_table.num_rows))


    def delete_dataset(self):
        self.bigquery_client.delete_dataset(
            self.dataset, delete_contents=True, not_found_ok=True
        )

    def run_query(self, query):
        bytes_scanned = 0
        query_runtime = 0

        query = query.replace('PROJECT_NAME', self.project_name)
        query = query.replace('DATASET_NAME', self.dataset_name)
        job_config = bigquery.QueryJobConfig(use_query_cache=False)
        query_job = self.bigquery_client.query(query, job_config=job_config)
        try:
            query_job.result()
            query_runtime = query_job.ended - query_job.started
            bytes_scanned = query_job.total_bytes_billed
        except:
            print("Query " + query)
            print("FAILED")

        return (query_runtime.total_seconds() * 1000, bytes_scanned)

if __name__ == '__main__':
    f = open(sys.argv[1], 'r')
    params = yaml.safe_load(f)
    client = BigQueryClient(
            params['project_name'],
            params['input_bucket'],
            params['input_folder'],
            params['dataset_name'],
            )

    # setup
    if params['dataset_creation']:
        client.delete_dataset()
        client.create_dataset()
        for table_name in params['table_names']:
            client.create_table(table_name, params['format'])

    else:
        results = {}
        total_runtime = 0
        cost_per_megabyte = 5/1024/1024
        total_cost = 0
        total_bytes_scanned = 0
        file_list = []
        for sql_file in params['queries']:
            query_file = open(params['query_folder'] + '/' + sql_file, 'r')
            query = query_file.read()
            # if params['analyze']:
            #     query = "explain analyze " + query
            print("Running query", sql_file.split('.')[0], "...")
            (time_taken, bytes_scanned) = client.run_query(query)
            print("Query", sql_file.split('.')[0], " finished")
            file_list.append(sql_file.split('.')[0])
            total_runtime += time_taken
            total_bytes_scanned += bytes_scanned/1024/1024
            total_cost += bytes_scanned/1024/1024 * cost_per_megabyte
            results[sql_file.split('.')[0]] = {
                    "Time[s]" : time_taken / 1000,
                    "MB scanned" : bytes_scanned/1024/1024,
                    "Cost" : bytes_scanned/1024/1024 * cost_per_megabyte
            }

        results["Total"] = {
                "Time[s]" : total_runtime / 1000,
                "MB scanned" : total_bytes_scanned,
                "Cost" : total_cost
                }

        json_string = json.dumps(results)
        f = open("results.txt", "w")
        f.write(json_string)
        current_directory = os.getcwd()
        final_directory = os.path.join(
                current_directory,
                'experiments-' +  str(datetime.datetime.now()))
        if not os.path.exists(final_directory):
               os.mkdir(final_directory)

        os.rename(
                os.path.join(current_directory, "results.txt"),
                os.path.join(final_directory, "results.txt"))
        # if params['analyze']:
        #     for file_name in file_list:
        #         os.rename(
        #                 os.path.join(current_directory, file_name + ".txt"),
        #                 os.path.join(final_directory, file_name + ".txt"))
