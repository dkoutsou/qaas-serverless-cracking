from google.cloud import bigquery
import tables

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.
table_id = "cloudml-301409.tpch_sf_1_csv.part"

job_config = bigquery.LoadJobConfig(
    schema= [
        bigquery.SchemaField("p_partkey", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("p_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("p_mfgr", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("p_brand", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("p_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("p_size", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("p_container", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("p_retailprice", "NUMERIC", mode="REQUIRED"),
        bigquery.SchemaField("p_comment", "STRING", mode="REQUIRED"),
    ],
    field_delimiter='|',
    source_format=bigquery.SourceFormat.CSV,
)
uri = "gs://dimi-tpch-data/tpch-sf-1-csv/part/part*.tbl"

load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.

load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)  # Make an API request.
print("Loaded {} rows.".format(destination_table.num_rows))
