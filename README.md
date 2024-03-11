Thie project is an Airflow project by Paulina R and is a part od Data Engineering Nanodegree with Udacity.
The peoject uses the same underlying datasets that are in the Data Warehouse project with Udacity, but automates the pipeline in Airflow, with a schedule to run every hour. 

# Project Description
The project automates the data pipelines using Airflow + Python.
1. The main file final_project.py first stages data from s3 buckets into Redshift serveless data warehouse.
2. Then the fact table is constructed based on the staging tables and subsequently the dimension tables are created.
3. SQL statements that are executed to achieve these goals are stored in the final_project_sql_statements.py.
4. In the end of the project there is an implementation of data_quality.py file which checks number of rows in the tables. 

# Requirements
Before running the script make sure that: 
1. The you make a folder called final_project_operators_and_sql and put all files that are in this repository, but the final_projecy.py file.
2. Then put this project in a one above directory than the mentioned folder. 
