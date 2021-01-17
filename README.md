# Cloud Data Lake with S3 and Spark
### A project submitted as work for Udacity's Data Engineering Nano Degree 

## Introduction 
Sparkify is a startup providing music streaming service. Their application stores data in JSON format. 

We want to use S3 on AWS as a datalake to store both raw data as well as the fact, dimension tables in parquet format. 
We are using pyspark as our ETL pipeline to store the fact and dimension tables in parquet format. 
From there, our analysts can use any analytical tool capable of reading parquest files to perform analysis 
of user trends, listening trends etc.  


## Summary
We are building a data lake on AWS's S3 platform. 
We load "song data" and "log data" (JSON files) from S3 bucket into spark dataframes.  
Once data is loaded, we can run "ETL" which is a process of transforming and loading data in fact and dimension tables via pyspark. 
Finally, we save the fact and dimensions as parquet files on S3. 

All steps are executed via `etl.py` . Be sure to see detailed instructions below on how to setup this pyspark program.   


### Staging dataframes - song_data, log_data:
* staging dataframe song_data follows the structure of the song data: 
    ```
    {
        "num_songs": 1, 
        "artist_id": "ARJIE2Y1187B994AB7", 
        "artist_latitude": null, 
        "artist_longitude": null, 
        "artist_location": "", 
        "artist_name": "Line Renaud", 
        "song_id": "SOUPIRU12A6D4FA1E1", 
        "title": "Der Kleine Dompfaff", 
        "duration": 152.92036, 
        "year": 0
    }
    ```
* staging dataframe log_data follow the structure of the log data:
    ```
    {
        "artist":"Des'ree",
        "auth":"Logged In",
        "firstName":"Kaylee",
        "gender":"F",
        "itemInSession":1,
        "lastName":"Summers",
        "length":246.30812,
        "level":"free",
        "location":"Phoenix-Mesa-Scottsdale, AZ",
        "method":"PUT",
        "page":"NextSong",
        "registration":1540344794796.0,
        "sessionId":139,
        "song":"You Gotta Be",
        "status":200,
        "ts":1541106106796,
        "userAgent":"\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/35.0.1916.153 Safari\/537.36\"",
        "userId":"8"
    }
    ```

### Fact table:
* Fact table `songplays` describes the facts about each "songplay" event and has the following schema. 
This table is partitioned by year and month to make sure it remains fast as data grows over the years. 
It can be used to analyze the listening trends of users, memebership upgrade patters as well as artist trends  
```
 |-- start_time: timestamp (nullable = true)
 |-- user_id: string (nullable = true)
 |-- level: string (nullable = true)
 |-- song_id: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- session_id: long (nullable = true)
 |-- location: string (nullable = true)
 |-- user_agent: string (nullable = true)
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- songplay_id: integer (nullable = true)
```


### Dimenstion tables: 
* Dimension table `time` is your classic time dimension translating timestamp into day, month, year, week etc.
This table is partitioned by year and month to make sure it remains fast as data grows over the years. 
The table schema was prescribed in this project's specification and follows what was done in a redshift data warehouse.
```
 |-- start_time: timestamp (nullable = true)
 |-- hour: integer (nullable = true)
 |-- day: integer (nullable = true)
 |-- week: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- year: integer (nullable = true)
 |-- weekday: integer (nullable = true)
```


* Dimension table `users` describes each user and their leve in sparkify service. 
The table schema was prescribed in this project's specification and follows what was done in a redshift data warehouse.
```
 |-- user_id: string (nullable = true)
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- level: string (nullable = true)
```


* Dimension table `songs` describes each song. Joining songs and artists tables to fact table will allow use to analyze.
This table is partitioned by year and artist to make sure it remains fast as data grows over the years. 
trends. The table schema was prescribed in this project's specification and follows what was done in a redshift data warehouse.
```
 |-- song_id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- year: long (nullable = true)
 |-- duration: double (nullable = true)
```


* Dimension table `artists` describes each artist. 
The table schema was prescribed in this project's specification and follows what was done in a redshift data warehouse.
```
 |-- artist_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- location: string (nullable = true)
 |-- latitude: double (nullable = true)
 |-- longtitude: double (nullable = true)
```


## Installing and running this application


* Checkout code
```
git clone git@github.com:firozkabir/data-lake-s3.git
```


* Update configuration
```
[aws-keys]
aws_access_key_id="AKIAIOSFODNN7EXAMPLE"
aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

[s3-buckets]
input_data="s3://udacity-dend/"
output_data="s3://some-bucket/"
```


* Install dependencies
```
cd data-lake-s3
virtualenv -p /usr/bin/python3 venv
source venv/bin/activate
pip3 install -r requirements.txt
```


* Run the application interactively 
```
./etl.py
```


## Prepare Local Development Environment 

* Create virtual environment 

```bash
virtualenv -p /usr/bin/python3 venv
```

* Install dependencies 
```bash
source venv/bin/activate
pip3 install pyspark
```


* Update `dl.cfg`, you can use local file system for `input_data` and `output_data` for development 
```
[aws-keys]
aws_access_key_id="AKIAIOSFODNN7EXAMPLE"
aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

[s3-buckets]
input_data="s3://udacity-dend/"
output_data="s3://some-bucket/"
```

* Run application locally
```
source venv/bin/activate
./etl.py
```
