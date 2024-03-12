# Automating Data Pipelines with Airflow

This project is an Airflow project by Paulina R and is a part od Data Engineering Nanodegree with Udacity.<br>
The project uses the same underlying datasets that are in the Data Warehouse project with Udacity, but automates the pipeline in Airflow, with a schedule to run every hour. <br>
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines. They need high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They came to the conclusion that the best tool to achieve this is Apache Airflow. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets. <br>
The source data resided in S3 and needed to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consisted of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to. I created custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

# Implementation
The project automates the data pipelines using Airflow + Python.<br>
1. The main file final_project.py first stages data from s3 buckets into Redshift serveless data warehouse.<br>
2. Then the fact table is constructed based on the staging tables and subsequently the dimension tables are created.<br>
3. SQL statements that are executed to achieve these goals are stored in the final_project_sql_statements.py.<br>
4. In the end of the project there is an implementation of data_quality.py file which checks number of rows in the tables. <br>

# DAG

In the DAG, the default parameters were added according to these guidelines:<br>
* The DAG does not have dependencies on past runs<br>
* On failure, the task are retried 3 times<br>
* Retries happen every 5 minutes<br>
* Catchup is turned off<br>
* Do not email on retry<br>

The task dependencies were configured so that after the dependencies are set, the graph view follows the flow shown in the image below:
<img width="836" alt="Airflow_automate_data_pipelines_DAG" src="https://github.com/paulinaruda/airflow_automating_data_pipelines/assets/84568114/e5457a43-cb96-464d-8fd4-94f0214b6ec0">

# Prerequisites
Before running the script make sure that: <br>
1. The you make a folder called final_project_operators_and_sql and put all files that are in this repository, but the final_project.py file.<br>
2. Then put the final_project.py in a directory above the mentioned folder. <br>
3. Create an IAM User in AWS. <br>
4. Configure Redshift Serverless in AWS. <br>
5. Connect Airflow and AWS. <br>
6. Connect Airflow to AWS Redshift Serverless <br>

