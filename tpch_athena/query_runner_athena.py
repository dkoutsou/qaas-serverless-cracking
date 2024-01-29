import boto3
import time
import yaml
import sys
import json
import os
import datetime

class AWSClient:
    def __init__(self, output_bucket, output_folder):
        self.athena_client = boto3.client('athena')
        self.s3_client = boto3.client('s3')
        self.output_bucket = output_bucket
        self.output_folder = output_folder
        self.config = {'OutputLocation' :
                's3://' + str(output_bucket) + '/' + output_folder}
        self.athena_client = boto3.client('athena')


    def start_query(self, query_name, query, database=None, analyze=False):
        if database:
            response = self.athena_client.start_query_execution(
                    QueryString=query,
                    QueryExecutionContext = {
                        'Database': database
                    },
                    ResultConfiguration=self.config,
                )
        else:
            response = self.athena_client.start_query_execution(
                    QueryString=query,
                    ResultConfiguration=self.config
                )

        query_execution_id = response['QueryExecutionId']
        return self.wait_for_response(query_name, query, query_execution_id,
                                      analyze)

    def wait_for_response(self, query_name, query, query_execution_id, analyze):
        bytes_scanned = 0
        query_runtime = 0
        while True:
            query_response = self.athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id)
            status = query_response['QueryExecution']['Status']['State']
            if status in ['FAILED', 'CANCELLED']:
                print("Query " + query, status)
                break
            if status == 'SUCCEEDED':
                query_runtime = query_response['QueryExecution'] \
                    ['Statistics']['TotalExecutionTimeInMillis']
                bytes_scanned = query_response['QueryExecution'] \
                    ['Statistics']['DataScannedInBytes']
                if analyze:
                    self.download_file(query_name, query_execution_id)
                break
            time.sleep(1.0)

        return (query_runtime, bytes_scanned)

    def download_file(self, query_name, query_execution_id):
        res = self.s3_client.download_file(
                self.output_bucket,
                self.output_folder + query_execution_id + ".txt",
                query_name + ".txt")

if __name__ == '__main__':
    f = open(sys.argv[1], 'r')
    params = yaml.safe_load(f)
    client = AWSClient(
            params['output_bucket'],
            params['output_folder'],
            )
    query_folder = params['query_folder']

    results = {}
    total_runtime = 0
    cost_per_megabyte = 5/1024/1024
    total_cost = 0
    total_bytes_scanned = 0
    file_list = []
    if params['schema_creation']:
        if params['clear_schema']:
            client.start_query(
                    'drop_schema', 'DROP SCHEMA IF EXISTS ' + params['schema_name'] + ' CASCADE')
            client.start_query(
                    'create_schema', 'CREATE SCHEMA ' + params['schema_name'])
        for sql_file in params['queries']:
            query_file = open(query_folder + '/' + sql_file, 'r')
            query = query_file.read()
            word = "TABLE"
            query += "LOCATION 's3://" + params['input_bucket'] + \
                     "/" + params['input_folder'] + \
                     sql_file.split('.')[0] +"/'"
            (time_taken, bytes_scanned) = client.start_query(
                    sql_file.split('.')[0],
                    query,
                    params['schema_name'])
            print("Query", sql_file.split('.')[0], "finished")
    else:
        for schema in params['schemas']:
            for sql_file in schema['queries']:
                query_file = open(query_folder + '/' + sql_file, 'r')
                query = query_file.read()
                if params['analyze']:
                    query = "explain analyze " + query
                print("Running query", sql_file.split('.')[0], "...")
                (time_taken, bytes_scanned) = client.start_query(
                        sql_file.split('.')[0],
                        query,
                        schema['name'],
                        params['analyze'])
                file_list.append(sql_file.split('.')[0])
                total_runtime += time_taken / 1000
                total_bytes_scanned += bytes_scanned/1024/1024
                total_cost += bytes_scanned/1024/1024 * cost_per_megabyte
                results[sql_file.split('.')[0]] = {
                        "Time[s]" : time_taken / 1000,
                        "MB scanned" : bytes_scanned/1024/1024,
                        "Cost" : bytes_scanned/1024/1024 * cost_per_megabyte
                }
                print("Query", sql_file.split('.')[0], " finished")

            results["Total"] = {
                    "Time[s]" : total_runtime,
                    "MB scanned" : total_bytes_scanned,
                    "Cost" : total_cost
                    }

            json_string = json.dumps(results)
            f = open("results.txt", "w")
            f.write(json_string)
            current_directory = os.getcwd()
            final_directory = os.path.join(
                    current_directory,
                    params['experiment_name']+ '-' + str(sys.argv[2])
                    )
            if not os.path.exists(final_directory):
                   os.mkdir(final_directory)

            os.rename(
                    os.path.join(current_directory, "results.txt"),
                    os.path.join(final_directory, "results.txt"))
            if params['analyze']:
                for file_name in file_list:
                    os.rename(
                            os.path.join(current_directory, file_name + ".txt"),
                            os.path.join(final_directory, file_name + ".txt"))
