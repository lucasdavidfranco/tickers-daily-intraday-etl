# tickers-daily-intraday-etl

## Contents
1. [Introduction](#introduction)
2. [Configuration](#configuration)
3. [Autors](#autors)

## Introduction

This github-repository execute an ETL (Extract, Transform and Load) process which consists on getting stocks market data from https://www.alphavantage.co/ and https://twelvedata.com/ through API requests and then uploading this data to staging tables which are used to execute more complex transformation and get analytics tables. Tools involved in this ETL are Apache Airflow, APIs, Docker, Python and Redshift.

## Configuration

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
