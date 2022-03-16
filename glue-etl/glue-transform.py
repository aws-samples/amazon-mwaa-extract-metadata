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

import sys
from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


# Executes SQL Statement and returns Glue DynamicFrame of results
def spark_sql_query(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Reads input data from S3 path and stores into a DynamicFrame
input_data = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            "s3://<airflow-extract-metadata-account-id>/optimised/"
        ],
        "recurse": True,
    },
    transformation_ctx="input_data",
)

# Re-map columns to business friendly names and appopriate data types
mapped_data = ApplyMapping.apply(
    frame=input_data,
    mappings=[
        ("State", "string", "State", "string"),
        ("Phone", "string", "Phone", "string"),
        ("Total_Charge#8", "string", "Total_Charge", "double"),
    ],
    transformation_ctx="mapped_data",
)

# SQL query to perform aggregation against mapped_data DynamicFrame
sum_total_charge_by_state_phone_sql = """select State,Phone,sum(Total_Charge) from mapped_data
group by State,Phone"""

# Execute SQL query and store into a DynamicFrame
aggregated_data = spark_sql_query(
    glueContext,
    query=sum_total_charge_by_state_phone_sql,
    mapping={"mapped_data": mapped_data},
    transformation_ctx="aggregated_data",
)

# Write DynamicFrame to S3 Location
output_data = glueContext.write_dynamic_frame.from_options(
    frame=aggregated_data,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://<airflow-extract-metadata-account-id>/conformed/", "partitionKeys": []},
    format_options={"compression": "gzip"},
    transformation_ctx="output_data",
)

job.commit()
