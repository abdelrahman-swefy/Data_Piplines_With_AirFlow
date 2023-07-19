# Data Pipelines with Airflow 

## Project Description
This project involves building data pipelines for a music streaming company, Sparkify, using Apache Airflow. The goal is to create high-grade data pipelines that are dynamic, reusable, and monitored, while also ensuring data quality through tests against the datasets. The source data is stored in S3, and it needs to be processed in Sparkify's data warehouse on Amazon Redshift. The datasets consist of JSON logs containing user activity and JSON metadata about the songs users listen to.


## DAG Configuration

In the DAG file, the following default parameters are set:

The DAG does not have dependencies on past runs.
On failure, the tasks are retried 3 times.
Retries occur every 5 minutes.
Catchup (backfilling) is turned off.
No emails are be sent on retry.
The task dependencies are be configured to match the flow shown in the provided image of the project DAG in the Airflow UI.

## Building the Operators

The project requires building four different operators to stage the data, transform the data, and perform data quality checks. These operators will run SQL statements against the Redshift database. While building the operators, it's essential to utilize Airflow's built-in functionalities such as connections and hooks to simplify the process.

## Stage Operator

The stage operator loads any JSON-formatted files from S3 to Amazon Redshift. It creates and executes a SQL COPY statement based on the provided parameters. The parameters specifies the S3 file location and the target table in Redshift. The stage operator also supports templated fields to load timestamped files from S3 based on the execution time and support backfilling.

## Fact and Dimension Operators

The fact and dimension operators will utilize the provided SQL helper class for data transformations. The logic for most transformations will reside within the SQL statements. The operators takes as input a SQL statement, the target database, and the target table for the query results. For dimension loads, the truncate-insert pattern where the target table is emptied before loading, is handled. The operators also supports append functionality for fact tables.

## Data Quality Operator

The data quality operator is responsible for running checks on the data to ensure its quality. It receives one or more SQL-based test cases along with the expected results and execute the tests. The operator compares the test results to the expected results and raises an exception if there is a mismatch. This will cause the task to retry and eventually fail if the issue persists.
