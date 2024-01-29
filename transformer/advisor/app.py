import json
import boto3
import datetime
import pytz
import time
import math
import pyarrow as pa
import pyarrow.csv
import pyarrow.parquet as pq
from pyarrow import fs

def wait_until(end_datetime):
    while True:
        diff = (end_datetime - datetime.datetime.now(tz=pytz.UTC)).total_seconds()
        if diff <= 0:
            return
        else:
            time.sleep(diff/2)

def advisor(event, context):
    worker_id = event['worker_id']
    action = event['action']
    logging = event['logging']
    logs = []
    if logging:
        logs.append("worker_id {0}, worker_start, {1}, end, {2}".format(
            worker_id, event['action'], datetime.datetime.now()))
        logs.append("worker_id {0}, worker_total, {1}, start, {2}".format(
            worker_id, event['action'], datetime.datetime.now()))
    if action == "no_op":
        if logging:
            logs.append("worker_id {0}, {1}, start, {2}".format(
                worker_id, event['action'], datetime.datetime.now()))
        time_format = "%Y-%m-%dT%H:%M:%S.%f%z"
        end_obj = datetime.datetime.strptime(event['endtime'], time_format)
        wait_until(end_obj)
    elif action == "compress":
        filename = event['filename']
        if logging:
            logs.append("worker_id {0}, {1}, table {2}, start, {3}".format(
                worker_id, event['action'], filename, datetime.datetime.now()))
        bucket = event['bucket']
        input_path = event['input_path']
        output_path = event['output_path']
        schema = event['schema']
        s3_filesystem = fs.S3FileSystem()
        s3_input_path = bucket + "/" + \
                input_path + filename
        if logging:
            logs.append("worker_id {0}, read_file, table {1}, start, {2}".format(
                                                        worker_id,
                                                        filename,
                                                        datetime.datetime.now()))
        s3_input_file = s3_filesystem.open_input_stream(s3_input_path)
        table = pyarrow.csv.read_csv(
                s3_input_file,
                parse_options=pyarrow.csv.ParseOptions(delimiter='|'),
                read_options=pyarrow.csv.ReadOptions(column_names=schema))
        if logging:
            logs.append("worker_id {0}, read_file, table {1}, end, {2}".format(
                                                        worker_id,
                                                        filename,
                                                        datetime.datetime.now()))
        s3_output_path = bucket + "/" + \
                output_path + filename.split(".")[0] + "." + \
                filename.split(".")[2] + ".parquet"
        if logging:
            logs.append("worker_id {0}, write_file, table {1}, start, {2}".format(
                                                        worker_id,
                                                        filename,
                                                        datetime.datetime.now()))
        s3_output_file = s3_filesystem.open_output_stream(s3_output_path)
        pq.write_table(table, s3_output_file)
        if logging:
            logs.append("worker_id {0}, write_file, table {1}, end, {2}".format(
                                                        worker_id,
                                                        filename,
                                                        datetime.datetime.now()))
            logs.append("worker_id {0}, {1}, table {2}, end, {3}".format(
                worker_id, event['action'], filename, datetime.datetime.now()))
    elif action == "split":
        filename = event['filename']
        if logging:
            logs.append("worker_id {0}, {1}, table {2}, start, {3}".format(
                worker_id, event['action'], filename, datetime.datetime.now()))
        bucket = event['bucket']
        input_path = event['input_path']
        output_path = event['output_path']
        num_batches = event['num_batches']
        file_format = event['file_format']
        s3_filesystem = fs.S3FileSystem()
        s3_input_path = bucket + "/" + \
                input_path + filename
        s3_input_file = s3_filesystem.open_input_file(s3_input_path)
        if file_format == 'parquet':
            parquet_file = pq.ParquetFile(s3_input_file)
            num_rows = parquet_file.metadata.num_rows
            batch_size = math.ceil(num_rows / num_batches)
        else:
            file_size = s3_filesystem.get_file_info(s3_input_path).size
            batch_size = math.ceil(file_size / num_batches)


        if logging:
            logs.append("worker_id {0}, split_write, table {1}, start, {2}".format(
                                                        worker_id,
                                                        filename,
                                                        datetime.datetime.now()))
        output_prefix = filename.split(".")[0]
        s3_output_path = bucket + "/" + output_path

        if file_format == 'parquet':
            for i, batch in enumerate(parquet_file.iter_batches(batch_size=batch_size)):
                if logging:
                    logs.append("worker_id {0}, loop, table {1}, start, {2}".format(
                                                                worker_id,
                                                                filename,
                                                                datetime.datetime.now()))
                table = pa.Table.from_batches([batch])
                output_file = f"{output_prefix}_batch_{i}.parquet"
                if logging:
                    logs.append("worker_id {0}, write_file, table {1}, start, {2}".format(
                                                                worker_id,
                                                                filename,
                                                                datetime.datetime.now()))
                output_path = s3_output_path + output_file
                s3_output_file = s3_filesystem.open_output_stream(output_path)
                pq.write_table(table, s3_output_file)
                if logging:
                    logs.append("worker_id {0}, write_file, table {1}, end, {2}".format(
                                                                worker_id,
                                                                filename,
                                                                datetime.datetime.now()))
                    logs.append("worker_id {0}, loop, table {1}, end, {2}".format(
                                                                worker_id,
                                                                filename,
                                                                datetime.datetime.now()))


        elif file_format == 'csv':
            reader = pyarrow.csv.open_csv(
                            s3_input_file,
                            read_options=pa.csv.ReadOptions(block_size=batch_size),
                            parse_options=pa.csv.ParseOptions(delimiter='|'),
                        )
            index = 0
            while True:
                try:
                    if logging:
                        logs.append("worker_id {0}, read_file, table {1}, start, {2}".format(
                                                                    worker_id,
                                                                    filename,
                                                                    datetime.datetime.now()))
                    batch = reader.read_next_batch()
                    if logging:
                        logs.append("worker_id {0}, read_file, table {1}, end, {2}".format(
                                                                    worker_id,
                                                                    filename,
                                                                    datetime.datetime.now()))
                    output_file = f"{output_prefix}_batch_{index}.tbl"

                    if logging:
                        logs.append("worker_id {0}, write_file, table {1}, start, {2}".format(
                                                                    worker_id,
                                                                    filename,
                                                                    datetime.datetime.now()))
                    output_path = s3_output_path + output_file
                    s3_output_file = s3_filesystem.open_output_stream(output_path)
                    pyarrow.csv.write_csv(batch,
                                          s3_output_file,
                                          write_options=pa.csv.WriteOptions(
                                              include_header=False)
                                          )
                    if logging:
                        logs.append("worker_id {0}, write_file, table {1}, end, {2}".format(
                                                                    worker_id,
                                                                    filename,
                                                                    datetime.datetime.now()))
                    index += 1
                except StopIteration:
                    break


        if logging:
            logs.append("worker_id {0}, split_write, table {1}, end, {2}".format(
                                                            worker_id,
                                                            filename,
                                                            datetime.datetime.now()))


        if logging:
            logs.append("worker_id {0}, {1}, table {2}, end, {3}".format(
                worker_id, event['action'], filename, datetime.datetime.now()))
    elif action == "sort":
        filename = event['filename']
        if logging:
            logs.append("worker_id {0}, {1}, table {2}, start, {3}".format(
                worker_id, event['action'], filename, datetime.datetime.now()))
        bucket = event['bucket']
        input_path = event['input_path']
        output_path = event['output_path']
        sort_column = event['sort_column']
        s3_filesystem = fs.S3FileSystem()
        s3_input_path = bucket + "/" + \
                input_path + filename
        if logging:
            logs.append("worker_id {0}, read_file, table {1}, start, {2}".format(
                                                        worker_id,
                                                        filename,
                                                        datetime.datetime.now()))
        s3_input_file = s3_filesystem.open_input_file(s3_input_path)
        table = pa.parquet.read_table(s3_input_file)



        if logging:
            logs.append("worker_id {0}, read_file, table {1}, end, {2}".format(
                                                        worker_id,
                                                        filename,
                                                        datetime.datetime.now()))

            logs.append("worker_id {0}, sort, table {1}, start, {2}".format(
                                                        worker_id,
                                                        filename,
                                                        datetime.datetime.now()))
        sorted_table = table.sort_by(sort_column)
        if logging:
            logs.append("worker_id {0}, sort, table {1}, end, {2}".format(
                                                        worker_id,
                                                        filename,
                                                        datetime.datetime.now()))
            logs.append("worker_id {0}, write_file, table {1}, start, {2}".format(
                                                        worker_id,
                                                        filename,
                                                        datetime.datetime.now()))
        output_prefix = filename.split(".")[0]
        s3_output_path = bucket + "/" + output_path

        output_file = f"sorted_{output_prefix}.parquet"
        output_path = s3_output_path + output_file
        s3_output_file = s3_filesystem.open_output_stream(output_path)
        pq.write_table(sorted_table, s3_output_file)
        if logging:
            logs.append("worker_id {0}, write_file, table {1}, end, {2}".format(
                                                        worker_id,
                                                        filename,
                                                        datetime.datetime.now()))

            logs.append("worker_id {0}, {1}, table {2}, end, {3}".format(
                worker_id, event['action'], filename, datetime.datetime.now()))
    elif action == "compress_split":
        filename = event['filename']
        if logging:
            logs.append("worker_id {0}, {1}, table {2}, start, {3}".format(
                worker_id, event['action'], filename, datetime.datetime.now()))
        bucket = event['bucket']
        input_path = event['input_path']
        output_path = event['output_path']
        num_batches = event['num_batches']
        schema = event['schema']
        s3_filesystem = fs.S3FileSystem()
        s3_input_path = bucket + "/" + \
                input_path + filename
        output_prefix = filename.split(".")[0]
        s3_output_path = bucket + "/" + output_path

        if logging:
            logs.append("worker_id {0}, split_write, table {1}, start, {2}".format(
                                                        worker_id,
                                                        filename,
                                                        datetime.datetime.now()))
        s3_input_file = s3_filesystem.open_input_file(s3_input_path)
        file_size = s3_filesystem.get_file_info(s3_input_path).size
        batch_size = math.ceil(file_size / num_batches)
        reader = pyarrow.csv.open_csv(
                        s3_input_file,
                        read_options=pa.csv.ReadOptions(block_size=batch_size,
                                                        column_names=schema),
                        parse_options=pa.csv.ParseOptions(delimiter='|'),
                    )
        index = 0
        while True:
            try:
                if logging:
                    logs.append("worker_id {0}, read_file, table {1}, start, {2}".format(
                                                                worker_id,
                                                                filename,
                                                                datetime.datetime.now()))
                batch = reader.read_next_batch()
                if logging:
                    logs.append("worker_id {0}, read_file, table {1}, end, {2}".format(
                                                                worker_id,
                                                                filename,
                                                                datetime.datetime.now()))
                    logs.append("worker_id {0}, write_file, table {1}, start, {2}".format(
                                                                worker_id,
                                                                filename,
                                                                datetime.datetime.now()))
                output_file = f"{output_prefix}_batch_{index}.parquet"
                output_path = s3_output_path + output_file
                s3_output_file = s3_filesystem.open_output_stream(output_path)
                table = pa.Table.from_batches([batch])
                pq.write_table(table, s3_output_file)
                if logging:
                    logs.append("worker_id {0}, write_file, table {1}, end, {2}".format(
                                                                worker_id,
                                                                filename,
                                                                datetime.datetime.now()))
                index += 1
            except StopIteration:
                break


        if logging:
            logs.append("worker_id {0}, split_write, table {1}, end, {2}".format(
                                                            worker_id,
                                                            filename,
                                                            datetime.datetime.now()))


            logs.append("worker_id {0}, {1}, table {2}, end, {3}".format(
                worker_id, event['action'], filename, datetime.datetime.now()))

    elif action == "all":
        filename = event['filename']
        if logging:
            logs.append("worker_id {0}, {1}, table {2}, start, {3}".format(
                worker_id, event['action'], filename, datetime.datetime.now()))
        bucket = event['bucket']
        input_path = event['input_path']
        output_path = event['output_path']
        sort_column = event['sort_column']
        num_batches = event['num_batches']
        sort_column = event['sort_column']
        schema = event['schema']
        s3_filesystem = fs.S3FileSystem()
        s3_input_path = bucket + "/" + \
                input_path + filename
        output_prefix = filename.split(".")[0]
        s3_output_path = bucket + "/" + output_path

        if logging:
            logs.append("worker_id {0}, split_sort_write, table {1}, start, {2}".format(
                                                        worker_id,
                                                        filename,
                                                        datetime.datetime.now()))
        s3_input_file = s3_filesystem.open_input_file(s3_input_path)
        file_size = s3_filesystem.get_file_info(s3_input_path).size
        batch_size = math.ceil(file_size / num_batches)
        reader = pyarrow.csv.open_csv(
                        s3_input_file,
                        read_options=pa.csv.ReadOptions(block_size=batch_size,
                                                        column_names=schema),
                        parse_options=pa.csv.ParseOptions(delimiter='|'),
                    )
        index = 0
        while True:
            try:
                if logging:
                    logs.append("worker_id {0}, read_file, table {1}, start, {2}".format(
                                                                worker_id,
                                                                filename,
                                                                datetime.datetime.now()))
                batch = reader.read_next_batch()
                if logging:
                    logs.append("worker_id {0}, read_file, table {1}, end, {2}".format(
                                                                worker_id,
                                                                filename,
                                                                datetime.datetime.now()))
                    logs.append("worker_id {0}, write_file, table {1}, start, {2}".format(
                                                                worker_id,
                                                                filename,
                                                                datetime.datetime.now()))
                output_file = f"{output_prefix}_batch_{index}.parquet"
                output_path = s3_output_path + output_file
                table = pa.Table.from_batches([batch])
                if logging:
                    logs.append("worker_id {0}, sort, table {1}, start, {2}".format(
                                                                worker_id,
                                                                filename,
                                                                datetime.datetime.now()))
                sorted_table = table.sort_by(sort_column)
                if logging:
                    logs.append("worker_id {0}, sort, table {1}, end, {2}".format(
                                                                worker_id,
                                                                filename,
                                                                datetime.datetime.now()))
                s3_output_file = s3_filesystem.open_output_stream(output_path)
                pq.write_table(table, s3_output_file)
                if logging:
                    logs.append("worker_id {0}, write_file, table {1}, end, {2}".format(
                                                                worker_id,
                                                                filename,
                                                                datetime.datetime.now()))
                index += 1
            except StopIteration:
                break


        if logging:
            logs.append("worker_id {0}, split_sort_write, table {1}, end, {2}".format(
                                                            worker_id,
                                                            filename,
                                                            datetime.datetime.now()))



            logs.append("worker_id {0}, {1}, table {2}, end, {3}".format(
                worker_id, event['action'], filename, datetime.datetime.now()))

    if logging:
        logs.append("worker_id {0}, worker_total, {1}, end, {2}".format(
            worker_id, event['action'], datetime.datetime.now()))
        s3_filesystem = fs.S3FileSystem()
        log_contents = '\n'.join(logs)
        s3_output_log_path = event['bucket'] + \
            "/logs/logs_worker" +  str(worker_id) + ".txt"
        s3_output_log_file = s3_filesystem.open_output_stream(s3_output_log_path)
        s3_output_log_file.write(log_contents.encode())
