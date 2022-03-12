## Persist and analyze metadata in a transient Amazon MWAA environment

This repository contains sample code for persist and analyze metadata in a transient Amazon MWAA environment

This blog post shows how to export, persist and analyse Airflow metadata in Amazon S3 enabling you to run and perform pipeline monitoring and analysis. In doing so, you can spin down Airflow instances without losing operational metadata.

As part of the included blog tutorial, you will be able to perform below high-level tasks:
  1) Run CloudFormation stack to create all necessary resources
  2) Trigger Airflow DAGs to perform sample ETL workload and generate operational metadata in back-end database
  3) Trigger Airflow DAG to export operational metadata into Amazon S3
  4) Perform analysis with Amazon Athena

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

