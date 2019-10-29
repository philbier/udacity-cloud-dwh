# Data Warehousing with AWS Redshift

The purpose of this project was to build a Data Warehouse using Amazon Redshift. Provided data in .JSON-format on Amazon S3 had to be extracted, analyzed, transformed and then loaded to Redshift (ETL process) into a specified database schema. 

## Database Schema
This is the target schema that will be build using `create_tables.py`. The model represents a classical star schema with songplays as fact table and all other tables as dimension tables.
!['Star Schema'](/img/dwh_model.PNG) 

## ETL Pipeline
Before data can be loaded or inserted into the DWH, it needs to be staged in to specified staging tables. Staging layers are a best practice to be independend on source systems.
!['Staging Model'](/img/staging_model.PNG)

## How to use
1. Launch a Redshift Cluster 
2. Configure `dwh.cfg` file with AWS credentials.
3. Run the scripts in `Run.ipnyb` (Jupyter Notebook)
    1. First import the relevant libraries
    2. Run `create_tables.py` file to create the
    3. Run `etl.py` file to start the ETL Process 

