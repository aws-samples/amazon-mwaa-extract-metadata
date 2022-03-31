# Persist and analyze metadata in a transient Amazon MWAA environment

This repository contains sample code for persisting and analyzing metadata in a transient Amazon MWAA environment. Storing this metadata in your data lake enables you to better perform pipeline monitoring and analysis. Tearing down instances whilst preserving the metadata enables you to further optimize the costs of Amazon MWAA.

This [blog](https://aws.amazon.com/blogs/big-data/persist-and-analyze-metadata-in-a-transient-amazon-mwaa-environment/) provides a detailed overview and step-by-step instructions on how to export, persist, and analyze Airflow metadata. 

## Solution Overview

The below diagram illustrates the solution architecture. Please note, Amazon QuickSight is NOT included as part of the CloudFormation stack in this repository. It has been placed in the diagram to illustrate that metadata can be visualized using a business intelligence tool.

![Figure 1 - Solution Architecture](/images/solution-architecture.png)


## Prerequisites

To implement the solution, you will need following :
- An [AWS Account](https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/)
- Basic understanding of [Apache Airflow](https://airflow.apache.org/), [Amazon Athena](https://aws.amazon.com/athena/?whats-new-cards.sort-by=item.additionalFields.postDateTime&whats-new-cards.sort-order=desc), [Amazon Simple Storage Service](https://aws.amazon.com/s3/) (Amazon S3), [AWS Glue](https://aws.amazon.com/glue/), [Amazon Managed Workflows for Apache Airflow](https://aws.amazon.com/managed-workflows-for-apache-airflow/) (MWAA) and [AWS Cloud Formation](https://aws.amazon.com/cloudformation/)

## Deploy Infrastructure

The provisioning takes about 30 minutes to complete. 

The CloudFormation template generates the following resources:
- VPC infrastructure that uses [public routing over the Internet](https://docs.aws.amazon.com/mwaa/latest/userguide/networking-about.html#networking-about-overview-public).
- Amazon S3 buckets required to support Amazon MWAA
- Amazon MWAA environment
- AWS Glue jobs for data processing and help generate airflow metadata
- [AWS Lambda-backed custom resources](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/template-custom-resources-lambda.html) to upload to Amazon S3 the sample data, AWS Glue scripts and DAG configuration files
- [AWS Identity and Access Management](https://aws.amazon.com/iam/) (IAM) users, roles, and policies


## What this repo contains

```
cloudformation/
  managed-airflow-cfn.yaml
dags/	
  extract_metadata.py
  run-simple-dags.py
  run-glue-jobs.py
glue-etl/
  glue-transform.py
  glue-csv-parquet.py
images/
  solution-architecture.png
CODE_OF_CONDUCT.md
CONTRIBUTING.md
LICENSE
README.md
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

