# Serverless cracking for QaaS

This repo provides instructions on how to run experiments for our vision paper "Serverless cracking for QaaS". First install the packages using the `pip install -r requirements.txt' command. Then you will have to set up your AWS and Google Cloud credentials.

## Transformer

The folder `transformer` contains the code for the AWS Lambda function that runs transformations on TPC-H. Assuming you have set up your AWS credentials here are some example instructions to set up the function:

```
cd transformer
sam build
sam deploy --s3-bucket <YOUR_S3_BUCKET> --stack-name advisor --capabilities CAPABILITY_IAM
```

You can also run the transformer using the `run_transformation.py` script in the `run_lambda` folder. The script takes arguments from a yaml file. We provide an example yaml file in the folder. 

Example command:
```
python run_transformation.py params.yaml
```

## Amazon Athena and BigQuery

The folders `tpch_athena` and `tpch_bigquery` contain the scripts `query_runner_athena.py` and `query_runner_bigqquery.py` that run TPC-H on Amazon Athena and BigQuery respectively. The whole execution happens with yaml files. We provide example yaml files for creating and running TPC-H. 

Example commands: 
```
python query_runner_athena.py creation.yaml
python query_runner_athena.py execution.yaml
```
