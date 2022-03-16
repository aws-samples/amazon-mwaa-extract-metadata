"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

from airflow import DAG  
from datetime import datetime
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator

with DAG(
    'glue-etl',
    start_date=datetime(2022, 2, 18),
    catchup=False,
    schedule_interval=None) as dag:

    glue_csv_to_parquet = AwsGlueJobOperator(
                          task_id="glue_csv_to_parquet",
                          job_name="glue-csv-to-parquet"
                          )

    glue_transform = AwsGlueJobOperator(
                          task_id="glue_transform",
                          job_name="glue-transform"
                          )

    glue_csv_to_parquet >> glue_transform