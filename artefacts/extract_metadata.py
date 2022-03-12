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

from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.session import create_session
from airflow.models import DagRun, TaskFail, TaskInstance
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from io import StringIO
from datetime import datetime
import csv

MAX_AGE_IN_DAYS = 30 
S3_BUCKET = '<airflow-extract-metadata-account-id>'

OBJECTS_TO_EXPORT = [
    [DagRun, DagRun.execution_date],
    [TaskFail, TaskFail.execution_date],
    [TaskInstance, TaskInstance.execution_date]
]


def export_db_fn():
    with create_session() as session:
        s3_folder_name = datetime.today().strftime('%Y-%m-%d')
        oldest_date = days_ago(MAX_AGE_IN_DAYS)

        s3_hook = S3Hook()
        s3_client = s3_hook.get_conn()

        for table_name, execution_date in OBJECTS_TO_EXPORT:
            query = session.query(table_name) \
                           .filter(execution_date >= days_ago(MAX_AGE_IN_DAYS))

            all_rows=query.all()
            name = table_name.__name__.lower()

            if len(all_rows) > 0:
                out_file_string = ""

                extract_file = StringIO(out_file_string)
                extract_file_writer = csv.DictWriter(extract_file, vars(all_rows[0]).keys())
                extract_file_writer.writeheader()

                for row in all_rows:
                    extract_file_writer.writerow(vars(row))

                S3_KEY = 'export/' + name + '/dt=' + s3_folder_name + '/' + name + '.csv'
                s3_client.put_object(Bucket=S3_BUCKET, Key=S3_KEY, Body=extract_file.getvalue())

with DAG(
    dag_id='db_export_dag', 
    schedule_interval=None, 
    catchup=False, 
    start_date=datetime(2022, 2, 18)
) as dag:

    export_db = PythonOperator(
        task_id='export_db', 
        python_callable=export_db_fn
    )