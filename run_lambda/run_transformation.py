import yaml
import logging
import pytz
import datetime
import sys
import boto3
import json
import time
import os
import numpy as np
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager, Process
from joblib import Parallel, delayed
from botocore.exceptions import ClientError

class RunTransformations:
    tables_schema = {
            'customer': [
                'c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone',
                'c_acctbal', 'c_mktsegment', 'c_comment',
            ],
            'lineitem': [
                'l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber',
                'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax',
                'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment',
            ],
            'nation': [
                'n_nationkey', 'n_name', 'n_regionkey', 'n_comment',
            ],
            'orders': [
                'o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice',
                'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority',
                'o_comment',
            ],
            'part': [
                'p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size',
                'p_container', 'p_retailprice', 'p_comment',
            ],
            'partsupp': [
                'ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost',
                'ps_comment',
            ],
            'region': [
                'r_regionkey', 'r_name', 'r_comment',
            ],
            'supplier': [
                's_suppkey', 's_name', 's_address', 's_nationkey', 's_phone',
                's_acctbal', 's_comment',
            ],
        }

    def __init__(self, log_group, input_path, bucket, output_folder,
                 function_name, function_cost, num_processes, num_files,
                 logging):
        self.log_group = log_group
        self.input_path = input_path
        self.bucket = bucket
        self.output_folder = output_folder
        self.function_name = function_name
        self.function_cost = float(function_cost)
        self.num_processes = num_processes
        self.num_files = num_files
        self.logging = logging
        self.worker_id_dict = {}
        self.invocation_delay = {}
        self.total_duration_dict = {}
        self.target_tz = pytz.timezone('UTC')

    def pause_until(self, end_datetime):
        while True:
            diff = (end_datetime - datetime.datetime.now(tz=pytz.UTC)).total_seconds()
            if diff <= 0:
                return
            else:
                time.sleep(diff/2)

    def delete_local_logs(self, file):
        """
        Delete a file locally.

        :param file_path: Path to the local file
        """
        try:
            os.remove(file)
            print(f"Deleted local file: {file}")
        except OSError as e:
            print(f"Error deleting file {file}. Error: {e}")

    def delete_logs_from_s3(self, s3_client, file_key):
        """
        Delete a file from S3.

        :param s3_client: boto3 S3 client
        :param bucket_name: S3 bucket name
        :param file_key: Key of the file in the S3 bucket
        """
        s3_client.delete_object(Bucket=self.bucket, Key=file_key)
        print(f"Deleted file {file_key} from S3 bucket {self.bucket}")

    def delete_log_streams(self):
        next_token = None
        logs = boto3.client('logs')

        log_groups = logs.describe_log_groups(logGroupNamePrefix=self.log_group)

        for log_group in log_groups['logGroups']:
            log_group_name = log_group['logGroupName']
            print("Deleting log streams...")

            while True:
                if next_token:
                    log_streams = logs.describe_log_streams(logGroupName=log_group_name,
                                                            nextToken=next_token)
                else:
                    log_streams = logs.describe_log_streams(logGroupName=log_group_name)

                next_token = log_streams.get('nextToken', None)

                for stream in log_streams['logStreams']:
                    log_stream_name = stream['logStreamName']
                    logs.delete_log_stream(logGroupName=log_group_name,
                                           logStreamName=log_stream_name)

                if not next_token or len(log_streams['logStreams']) == 0:
                    break

    def process_txt_file(self, file_path):
        """
        Process every line of the given .txt file.

        :param file_path: Path to the .txt file
        """
        blacklist = ['no_op', 'worker_start', 'worker_total']
        with open(file_path, 'r') as file:
            for line in file:
                # Split the input string by ','
                parts = line.split(',')

                # Extract worker_id
                worker_id_parts = parts[0].strip().split(' ')
                worker_id = int(worker_id_parts[1])

                # Extract action
                action = parts[1].strip().split(' ')[0]

                if action not in blacklist:
                    # Extract table
                    table = parts[2].strip().split(' ')[1].split('.')[0]

                    # Extract and parse datetime
                    date_time_str = parts[4].strip()
                    date_time_obj = datetime.datetime.strptime(
                        date_time_str, '%Y-%m-%d %H:%M:%S.%f')
                    date_time_obj_utc = date_time_obj.replace(tzinfo=pytz.UTC)


                    # Store information in dictionary
                    if worker_id not in self.worker_id_dict:
                        self.worker_id_dict[worker_id] = {}
                    if action not in self.worker_id_dict[worker_id]:
                        self.worker_id_dict[worker_id][action] = {}
                    if table not in self.worker_id_dict[worker_id][action]:
                        self.worker_id_dict[worker_id][action][table] = []

                    self.worker_id_dict[worker_id][action][table].append(date_time_obj_utc)
                elif action == 'worker_total':
                    ad_action = parts[2].strip()
                    date_time_str = parts[4].strip()
                    date_time_obj = datetime.datetime.strptime(
                        date_time_str, '%Y-%m-%d %H:%M:%S.%f')
                    date_time_obj_utc = date_time_obj.replace(tzinfo=pytz.UTC)
                    if worker_id not in self.total_duration_dict:
                        self.total_duration_dict[worker_id] = []
                    self.total_duration_dict[worker_id].append(
                            date_time_obj_utc)
                elif action == 'worker_start':
                    ad_action = parts[2].strip()
                    if ad_action != 'no_op':
                        date_time_str = parts[4].strip()
                        date_time_obj = datetime.datetime.strptime(
                            date_time_str, '%Y-%m-%d %H:%M:%S.%f')
                        date_time_obj_utc = date_time_obj.replace(tzinfo=pytz.UTC)
                        self.invocation_delay[worker_id]['worker_start'].append(
                                date_time_obj_utc)


    def download_log_files_from_s3(self, folder_path="logs/"):
        s3_client = boto3.client('s3')

        # List all objects within the specified folder
        result = s3_client.list_objects_v2(Bucket=self.bucket, Prefix=folder_path)
        for content in result.get('Contents', []):
            file = os.path.basename(content['Key'])
            if file.endswith('.txt'):
                print(f"Downloading {content['Key']}")
                s3_client.download_file(self.bucket, content['Key'], file)
                self.process_txt_file(file)
                # self.delete_local_logs(file)
                self.delete_logs_from_s3(s3_client, content['Key'])


    def milliseconds_since_epoch(self, time_string):
        dt = maya.when(time_string)
        seconds = dt.epoch
        return seconds * 1000

    def postprocessing(self, write, filename):
        self.download_log_files_from_s3()
        action_averages = {}
        action_raw = {}
        if self.logging:
            for worker_id, action in self.worker_id_dict.items():
                for action_key, action_data in action.items():
                    for table, time_data in action_data.items():
                        if len(time_data) > 1:
                            if action_key not in action_averages:
                                action_averages[action_key] = {}
                                action_raw[action_key] = {}
                            if table not in action_averages[action_key]:
                                action_averages[action_key][table] = []
                                action_raw[action_key][table] = []
                            timedelta = (time_data[1] - time_data[0]).total_seconds()
                            action_averages[action_key][table].append(timedelta)
                            action_raw[action_key][table].append([str(time_data[0]),
                                                                  str(time_data[1])])

        total_duration = 0
        for worker_id, timeList in self.total_duration_dict.items():
            timedelta = (timeList[1] - timeList[0]).total_seconds()
            total_duration += timedelta

        total_cost = self.function_cost * total_duration * 100
        print("Total cost: ", total_cost)

        return total_cost, action_averages, action_raw

    def delete_files(self):
        s3_client = boto3.client('s3')

        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket,
                                   Prefix=self.output_folder)


        print('Deleting transformed files...')
        for page in pages:
            for obj in page['Contents']:
                s3_client.delete_object(Bucket=self.bucket, Key=obj['Key'])



    def invoke_function(self, table, i, global_counter,
                        extra_dict, invocation_delay):
        lambda_client = boto3.client('lambda')
        lambda_payload = {
                        "action": extra_dict['action'],
                        "bucket": self.bucket,
                        "input_path": self.input_path + "/" + table + "/",
                        "output_path": self.output_folder + "/" + table + "/",
                        "worker_id": global_counter,
                        "logging": self.logging
                        }
        if extra_dict['action'] == "compress":
            lambda_payload["filename"] = table + ".tbl." + str(i)
            lambda_payload["schema"] =  self.tables_schema[table]
        elif extra_dict['action'] == "split":
            lambda_payload["num_batches"] = extra_dict['num_batches']
            lambda_payload["file_format"] = extra_dict['file_format']
            if extra_dict['file_format'] == 'parquet':
                lambda_payload["filename"] = table + "." + str(i) + ".parquet"
            else:
                lambda_payload["filename"] = table + ".tbl." + str(i)
        elif extra_dict['action'] == "sort":
            lambda_payload["filename"] = table + "." + str(i) + ".parquet"
            lambda_payload["sort_column"] = extra_dict['sort_column']
        elif extra_dict['action'] == "compress_split":
            lambda_payload["filename"] = table + ".tbl." + str(i)
            lambda_payload["schema"] =  self.tables_schema[table]
            lambda_payload["num_batches"] = extra_dict['num_batches']
        elif extra_dict['action'] == "all":
            lambda_payload["filename"] = table + ".tbl." + str(i)
            lambda_payload["schema"] =  self.tables_schema[table]
            lambda_payload["num_batches"] = extra_dict['num_batches']
            lambda_payload["sort_column"] = extra_dict['sort_column']
        else:
            raise ValueError('Action not supported.')



        if self.logging:
            self.invocation_delay[global_counter] = {'worker_start': [
                    datetime.datetime.now(self.target_tz)]}

        res = lambda_client.invoke(FunctionName=self.function_name,
                                   InvocationType='Event',
                                   Payload=json.dumps(lambda_payload))

    def invoke_lambda(self, tables, **kwargs):
        start = datetime.datetime.now(self.target_tz)
        with Manager() as manager:
            self.invocation_delay = manager.dict()
            with ProcessPoolExecutor(max_workers=int(self.num_processes)) as executor:
                for tab in range(len(tables)):
                    for i in range(1, int(self.num_files) + 1):
                        executor.submit(self.invoke_function, tables[tab], i,
                                        tab * self.num_files + i, kwargs, self.invocation_delay)
            end_inv = datetime.datetime.now(self.target_tz)
            self.invocation_delay = dict(self.invocation_delay)

        end_inv = datetime.datetime.now(self.target_tz)
        time.sleep(60)
        s3 = boto3.client('s3')
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket,
                                   Prefix=self.output_folder)

        last_modified = None
        for page in pages:
            for obj in page['Contents']:
                try:
                    if obj['LastModified'] > last_modified:
                        last_modified = obj['LastModified']
                except TypeError:
                    last_modified = obj['LastModified']



        print("Start time ",  start)
        print("End invocation ",  end_inv)
        inv_per_sec = len(tables) * self.num_files / (end_inv - start).total_seconds()
        print("Invocations per second ", inv_per_sec)
        print("Last modified ",  last_modified)
        total_time = last_modified - start
        print("Total conversion time: ", total_time)
        return str(total_time), inv_per_sec

    def invoke_warmup(self, worker_id):
        lambda_client = boto3.client('lambda')
        res = lambda_client.invoke(
                FunctionName=self.function_name,
                InvocationType='Event',
                Payload=json.dumps(
                        {
                        "action": "no_op",
                        "endtime": dt.isoformat(),
                        "worker_id": worker_id,
                        "logging" : self.logging,
                        "bucket" : self.bucket,
                        }
                    )
                )

    def warmup(self, num_calls):
        Parallel(n_jobs=int(self.num_processes), prefer='processes')(
                delayed(self.invoke_warmup)(i)
                for i in range(1, num_calls + 1))

if __name__ == '__main__':
    f = open(sys.argv[1], 'r')
    params = yaml.safe_load(f)
    client = RunTransformations(
            params['log_group'],
            params['input_path'],
            params['bucket'],
            params['output_folder'],
            params['function_name'],
            params['function_cost'],
            params['num_processes'],
            params['num_files'],
            params['logging']
            )

    tables = []
    for table in params['tables']:
        tables.append(table)
    experiment_result = {}
    experiment_result['tables'] = tables
    print("Running warmup...")
    if params['warmup']:
        num_functions = int(1.2 * len(params['tables']) * int(params['num_files']))
        dt = datetime.datetime.now(tz=pytz.UTC) + datetime.timedelta(seconds=35)
        client.warmup(num_functions)
        client.pause_until(dt)
    print("Finished running warmup...")
    print("Running transformation...")
    if params['action'] == "compress":
        exp_time, inv_per_sec = client.invoke_lambda(tables,
                                                     action=params['action']
                                                    )
    elif params['action'] == "split":
        exp_time, inv_per_sec = client.invoke_lambda(
                                                    tables,
                                                    action=params['action'],
                                                    file_format=params['file_format'],
                                                    num_batches=15
                                                    )
    elif params['action'] == "sort":
        exp_time, inv_per_sec = client.invoke_lambda(
                                                    tables,
                                                    action=params['action'],
                                                    sort_column=params['sort_column'],
                                                    )
    elif params['action'] == "compress_split":
        exp_time, inv_per_sec = client.invoke_lambda(
                                                    tables,
                                                    action=params['action'],
                                                    num_batches=15
                                                    )
    elif params['action'] == "all":
        exp_time, inv_per_sec = client.invoke_lambda(
                                                    tables,
                                                    action=params['action'],
                                                    num_batches=15,
                                                    sort_column=params['sort_column'],
                                                    )
    print("Finished running transformation...")
    experiment_result['time'] = exp_time
    experiment_result['invocations_per_sec'] = inv_per_sec
    print("Deleting files...")
    client.delete_files()
    print("Finished deleting files...")
    print("Running postprocessing...")
    cost, action_averages, action_raw = client.postprocessing(
        True,
        params['experiment_name'] + '-' + str(sys.argv[2]) + '-logs'
    )
    print("Finished postprocessing...")
    experiment_result['cost'] = cost
    if params['logging']:
        for action, action_data in action_averages.items():
            for table, time in action_data.items():
                experiment_result[action + "_" + table] = np.average(time)

        invocation_delay = []
        invocation_delay_raw = []
        for worker_id, worker_data in client.invocation_delay.items():
            for key, time_data in worker_data.items():
                invocation_delay.append((time_data[1] -
                                        time_data[0]).total_seconds())
                invocation_delay_raw.append(
                    [str(time_data[0]), str(time_data[1])])

        total_time = []
        total_time_raw = []
        for worker_id, worker_data in client.total_duration_dict.items():
                total_time.append((
                            worker_data[1] -
                            worker_data[0]).total_seconds())
                total_time_raw.append(
                    [str(time_data[0]), str(time_data[1])])


        experiment_result['quantities_raw'] = action_raw
        experiment_result['invocation_delay'] = np.average(invocation_delay)
        experiment_result['invocation_delay_raw'] = invocation_delay_raw
        experiment_result['total_time'] = np.average(total_time)
        experiment_result['total_time_raw'] = total_time_raw
    json_string = json.dumps(experiment_result)
    f = open("results.txt", "w")
    f.write(json_string)
    current_directory = os.getcwd()
    final_directory = os.path.join(
            current_directory,
            params['experiment_name'] + '-' + str(sys.argv[2])
            )
    if not os.path.exists(final_directory):
           os.mkdir(final_directory)

    os.rename(
            os.path.join(current_directory, "results.txt"),
            os.path.join(final_directory, "results.txt"))
