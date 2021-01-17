#!/usr/bin/env python3

import sys
import os
from datetime import datetime
import configparser

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType
from pyspark.sql.window import Window


def get_config(config_file='dl.cfg'):
    """
    gets aws and s3 configurations from dl.cfg file
    shows the configuration on stdout for debugging
    :param config_file:
    :return:
    """
    try:

        config = configparser.ConfigParser()
        config.read(config_file)

        aws_id = config['aws-keys']['aws_access_key_id'].strip('"').strip("'")
        aws_secret = config['aws-keys']['aws_secret_access_key'].strip('"').strip("'")
        input_data = config['s3-buckets']['input_data'].strip('"').strip("'")
        output_data = config['s3-buckets']['output_data'].strip('"').strip("'")

    except Exception as e:
        print(f"{datetime.now()} - error reading configs from {config_file}:")
        print(f"{e}")
        raise e

    print(f"\n{'-' * 15} configs found {'-' * 15}")
    print(f"aws_id: {aws_id}")
    print(f"aws_secret: {aws_secret[:4]}{'*' * (len(aws_secret) - 4)}")
    print(f"input_data: {input_data}")
    print(f"output_data: {output_data}")
    print(f"{'-' * 44}\n")

    os.environ['AWS_ACCESS_KEY_ID'] = aws_id
    os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret

    return input_data, output_data


def create_spark_session():
    """
    creates a local spark session, adds aws-hadoop package to the session
    :return: SparkSession object
    """
    try:
        spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .getOrCreate()

    except Exception as e:
        print(f"{datetime.now()} - error creating spark session:")
        print(f"{e}")
        raise e

    print(f"{datetime.now()} - spark session created")

    return spark


class IngeterType(object):
    pass


def process_song_data(spark, input_data, output_data):
    """
    Reads song_data from input_data S3 bucket,
    performs data wrangling,
    saves dimension tables songs and artists in parquet format in output_data S3 bucket
    :param spark: a SparkSession object
    :param input_data: an S3 bucket to get input data from
    :param output_data: an S3 bucket to save output data to
    :return:
    """
    
    song_schema = StructType([
        StructField("num_songs", IntegerType(), True),
        StructField("artist_id", StringType(), True),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_longitude", DoubleType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("year", IntegerType(), True),

    ])

    song_data = input_data + "/song_data/*/*/*"
    print(f"{datetime.now()} - reading song_data from {song_data}")
    df = spark.read.json(song_data, song_schema)
    print(f"{datetime.now()} - done reading song_data.")

    print(f" {datetime.now()} - extracting columns song_id, title, artist_id, \
                                year and duration to create dimension table 'songs'")
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")

    print(f"{datetime.now()} - writing dimension table 'songs' to parquet file \
                            {output_data + '/songs.parquet'} partitioned by year and artist_id")
    songs_table.write \
        .option("compression", "gzip") \
        .mode('overwrite') \
        .partitionBy("year", "artist_id").parquet(output_data + "/songs.parquet")

    print(f"{datetime.now()} - extracting columns artist_id, name, location, lattitude and longitude \
                                to create dimension table artists")
    artists_table = df.selectExpr("artist_id",
                                  "artist_name as name",
                                  "artist_location as location",
                                  "artist_latitude as latitude",
                                  "artist_longitude as longtitude")

    print(f"{datetime.now()} - writing dimension table artists to parquet file {output_data + '/artists.parquet'}")
    artists_table.write \
        .option("compression", "gzip") \
        .mode("overwrite") \
        .parquet(output_data + '/artists.parquet')

    print(f"{datetime.now()} - done processing song_data")


def process_log_data(spark, input_data, output_data):
    """
    Reads both song_data and log_data.
    Performs data wrangling.
    Creates fact table songplays.
    Creates dimension tables time, users.
    Saves each table in output_data S3 bucket in parquet format.
    :param spark: a SparkSession object
    :param input_data:
    :param output_data:
    :return:
    """

    log_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", IntegerType(), True),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("method", StringType(), True),
        StructField("page", StringType(), True),
        StructField("registration", DoubleType(), True),
        StructField("sessionId", IntegerType(), True),
        StructField("song", StringType(), True),
        StructField("status", IntegerType(), True),
        StructField("ts", LongType(), True),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True),


    ])

    log_data = input_data + "/log_data/*/*"
    print(f"{datetime.now()} - reading log_data from {log_data}")
    df = spark.read.json(log_data, log_schema)
    print(f"{datetime.now()} - done reading log_data.")

    print(f"{datetime.now()} - extracting columns user_id, first_name, last_name, gender, level \
                            to create dimension table users")
    users_table = df.where(df.userId.isNotNull()) \
        .selectExpr("userId as user_id",
                    "firstName as first_name",
                    "lastName as last_name",
                    "gender",
                    "level") \
        .distinct()

    print(f"{datetime.now()} - writing dimension table users to parquet files at {output_data + '/users.parquet'}")
    users_table.write \
        .option("compression", "gzip") \
        .mode("overwrite") \
        .parquet(output_data + '/users.parquet')

    get_datetime_udf = f.udf(lambda x: datetime.fromtimestamp(x / 1000).strftime('%Y-%m-%d %H:%M:%S'))

    print(f"{datetime.now()} - extracting columns start_time, hour, day, week, month, year, weekday \
                            to create dimension table time")

    df = df.withColumn('start_time', get_datetime_udf(df.ts).cast("timestamp"))
    time_table = df.where(df.start_time.isNotNull()) \
        .select("start_time") \
        .distinct()

    time_table = time_table.withColumn("hour", f.hour(f.col("start_time"))) \
        .withColumn("day", f.dayofmonth(f.col("start_time"))) \
        .withColumn("week", f.weekofyear(f.col("start_time"))) \
        .withColumn("month", f.month(f.col("start_time"))) \
        .withColumn("year", f.year(f.col("start_time"))) \
        .withColumn("weekday", f.dayofweek(f.col("start_time")))

    print(f"{datetime.now()} - writing dimension table time to parquet files at {output_data + '/time.parquet'}")
    time_table.write \
        .option("compression", "gzip") \
        .mode("overwrite") \
        .partitionBy(["year", "month"]) \
        .parquet(output_data + '/time.parquet')

    print(f"{datetime.now()} - filtering dataframe for page='NextSong' before creating the fact table songplays ")
    df = df.where(df.page == 'NextSong')

    song_schema = StructType([
        StructField("num_songs", IntegerType(), True),
        StructField("artist_id", StringType(), True),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_longitude", DoubleType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("year", IntegerType(), True),

    ])

    song_data = input_data + "/song_data/*/*/*"
    print(f"{datetime.now()} - read in song_data from {song_data} to use for fact table songplays")
    song_df = spark.read.json(song_data, song_schema)
    print(f"{datetime.now()} - done loading song_data.")

    print(f"{datetime.now()} - extracting columns from joined song and log datasets \
                                to create songplays table")

    songplays_table = df.join(song_df,
                              [df.song == song_df.title,
                               df.artist == song_df.artist_name],
                              how='inner') \
        .selectExpr("start_time",
                    "userId as user_id",
                    "level",
                    "song_id",
                    "artist_id",
                    "sessionId as session_id",
                    "location",
                    "userAgent as user_agent"
                    )

    songplays_table = songplays_table.withColumn("year", f.year(f.col("start_time"))) \
        .withColumn("month", f.month(f.col("start_time"))) \
        .withColumn("songplay_id", f.row_number().over(Window.orderBy("session_id")))

    print(f"{datetime.now} - write songplays table to parquet files partitioned by year and month")
    songplays_table.write \
        .option("compression", "gzip") \
        .mode("overwrite") \
        .partitionBy(["year", "month"]) \
        .parquet(output_data + "/songplays.parquet")

    print(f"{datetime.now()} - done processing log_data")


def main(argv):
    print(f"*** {datetime.now()} - {argv[0]} - start ***")

    config_file = "dl.cfg"

    print(f"\n{datetime.now()} - getting config from {config_file}")
    input_data, output_data = get_config()

    print(f"\n{datetime.now()} - setting up spark session ")
    spark = create_spark_session()

    print(f"\n{datetime.now()} - processing song_data ")
    process_song_data(spark, input_data, output_data)

    print(f"\n{datetime.now()} - processing log_data ")
    process_log_data(spark, input_data, output_data)

    print(f"=== {datetime.now()} - {argv[0]} - end ===")


if __name__ == "__main__":
    main(sys.argv)
