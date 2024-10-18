# tickers-daily-intraday-etl

## Contents
1. [Introduction](#introduction)
2. [Project structure](#project_structure)
3. [Project pipeline](#project_pipeline)
4. [Project tests](#project_tests)
5. [Project setup](#configuration)
5. [Autors](#autors)

## Introduction

This github-repository execute an ETL (Extract, Transform and Load) process which consists on getting stocks market data from https://www.alphavantage.co/ and https://twelvedata.com/ through API requests and then uploading this data to staging tables which are used to execute more complex transformation and get analytics tables. Tools involved in this ETL are Apache Airflow, APIs, Docker, Python and Redshift.
Final result is to obtain analytics data to analyze daily or intradiary stock values (open_value, high_value, low_value, close_value, volume_amount) by date or datetime (depending on the table) and also more complex metrics such as slowly moving average metrics based on 5 previous records, previous metric value as of to calculate deviation vs previous value

## Project structure 

```bash
├── .github/              # Module that runs tests on every pull-request and push to main
├── analytics/            # Module that contains analytics ETL
├── dags/                 # DAG definition
├── staging/              # Module that contains staging ETL
├── tasks/                # Module that contains tasks which are run on DAG
├── tests/                # Unit-tests to check ETL functions
├── utils/                # Module that initializes connections and retrieves enviroment variables
├── docker-compose-yaml   # Docker compose configuration
├── dockerfile            # Docker image definition
├── README.md             # Project documentation
├── requirements.txt      # Project dependencies
```

## Project pipeline 

### Staging:

It is the first layer of our project. On this layer this tasks are performed: 

- create_staging_tables.py: Checks table existence and creates necessary tables if they do not exit
- etl_staging_daily.py: Gets data from Alphavantage API through requests. Then performs necessary transformations and filter data to process only incremental data or historical if it is the first time to upload that ticker. After that using a SQL Alchemy engine uploads final processed data to staging daily table. Data uploaded consists on ticker, open_value, close_value, high_value, low_value, volume_amount, audit_datetime (timestamp of record upload) and event_date
- etl_staging_intradiary.py: Gets data from Alphavantage API through requests. Then performs necessary transformations and filter data to process only incremental data or historical if it is the first time to upload that ticker. After that using a SQL Alchemy engine uploads final processed data to staging intradiary table. Data uploaded consists on ticker, open_value, close_value, high_value, low_value, volume_amount, audit_datetime (timestamp of record upload) and event_datetime

### Analytics:

On this layer this tasks are performed more complex transformation with business logic: 

- create_analytics_tables.py: Checks table existence and creates necessary tables if they do not exit
- etl_dim_analytics.py: Gets data from Alphavantage API through requests. Then performs necessary transformations. After that using a SQL Alchemy it uploads to analytics dimensional table new updates of tickers if there are and changes is_current flag to old ticker dimension data. If there are no updates, it keeps dimensions with no changes. It has a SCD-2 structure 
- etl_fact_analytics.py: Gets data from staging table and performs several transformations to calculate new metrics such as slowly moving average on certain metrics or previous data value to calculate deviations. This ETL perform an incremental upload so as to avoid duplication or avoid calculate all history every day

### Tasks: 

This package is used to sort staging and analytics functions to be used on DAG definition.

- staging_run: Sets to run functions in this order. Create tables, upload intradiary data, upload daily data
- analytics_run: Sets to run functions in this order. Create tables, fact analytics etl, dim analytics etl

### Dags:

Here is defined main airflow configuration such as trigger, retry, dependencies
By default it is set a daily trigger from Monday to Friday at 21:00 GMT-00:00
It is also set that staging_run preceeds analytics_run and has 1 retry configuration

## Project tests 

Several tests are performed to check functions of the ETL process

### Staging:

- test_extract_staging_data: We check that API function works as we expect and delivers proper output for transformation
- test_transform_staging_data: We check that info retrived by API request is correctly transformed and delivers desired output for upload
- test_load_staging_data: We check that info delivered by transform data is correctly executed to_sql function if there is info to upload and if there are no updates, it checks that connection is closed

### Analytics:

- test_extract_dimension_data: We check that API function works as we expect and delivers proper output for transformation
- test_transform_dimension_data: We check that info retrived by API request is correctly transformed and delivers desired output for upload
- test_load_dimension_data: We check that info delivered by transform data is correctly executed to_sql function
- test_etl_analytics_data: We check that connection is set and is executed each table ETL

## Project setup

1. Clone repository:
    ```bash
    git clone https://github.com/lucasdavidfranco/tickers-daily-intraday-etl.git
    ```

2. Access project directory:
    ```bash
    cd tickers-daily-intraday-etl
    ```

3. Setup an .env file:
    ```bash

    You will need and .env on project root with following data:
    
    DB_USER=db_user
    DB_PASSWORD=db_password
    DB_HOST=db_host
    DB_PORT=db_port
    DB_NAME=db_name
    ALPHA_KEY=alpha_vantage_key
    TWELVE_KEY=twelve_data_key
    REDSHIFT_SCHEMA=table_schema
    AIRFLOW_UID=50000
    _AIRFLOW_WWW_USER_USERNAME=admin
    _AIRFLOW_WWW_USER_PASSWORD=admin
    
    ```

4. Start docker instance:
    ```bash
    docker-compose up
    ```

5. Once docker containers are up access through your browser http://localhost:8080:
    ```bash
    Username: admin
    Password: admin
    ```

## Autors

- **Lucas Franco** - [Tu GitHub](https://github.com/lucasdavidfranco)
