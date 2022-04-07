import configparser
from datetime import datetime
from email import header
import os

from numpy import int64
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, desc, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp, to_date, dayofweek

config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['default']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['default']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    This function creates and returns a spark session
    '''
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    This function processes song data by:
    1) Reading the JSON data from S3
    2) Extracts columns for songs and artist tables
    3) Saving the tables in parquet files in S3 Bucket

    The arguments include the spark session object, the input S3 path and output S3 path
    '''
    song_data = input_data+'song_data/*/*/*.json'
    df = spark.read.json(song_data)

    # Extract and write songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).distinct()

    songs_table \
        .write \
        .mode('overwrite') \
        .partitionBy('year', 'artist_id') \
        .parquet(output_data+'songs/')

    # Extract and write artists table
    artists_table = df.select('artist_id', 
                            col('artist_name').alias('name'), 
                            col('artist_location').alias('location'), 
                            col('artist_latitude').alias('lattitude'), 
                            col('artist_longitude').alias('longitude')).distinct()

    artists_table \
        .write \
        .mode('overwrite') \
        .parquet(output_data+'artists/')



def process_log_data(spark, input_data, output_data):
    '''
    This function processes logs data by:
    1) Reading the JSON log data from S3
    2) Extracts columns for users, time
    3) Loading previously saved songs_data from S3 and joining it with log data to get songs_play table
    3) Saving the tables in parquet files in S3 Bucket

    The arguments include the spark session object, the input S3 path and output S3 path
    '''
    log_data = input_data+'log_data/'
    df = spark.read.json(log_data)
    df = df.where(df.page == 'NextSong')

    # Extract and write users table    
    user_table = df.select(col('userId').alias('user_id'),
                           col('firstName').alias('first_name'), 
                           col('lastName').alias('last_name'), 
                           'gender', 
                           'level').sort(desc('ts')).drop_duplicates(subset=['userId'])

    user_table \
        .write. \
        mode('overwrite') \
        .parquet(output_data+'users/')

    # Extract and write time table.
    df = df.withColumn('timestamp', to_timestamp(df.ts/1000))
    df = df.withColumn('datetime', to_date(df.timestamp))

    time_table = df.select('timestamp').alias('start_time') \
        .withColumn('hour', hour('timestamp')) \
        .withColumn('day', dayofmonth('timestamp')) \
        .withColumn('week', weekofyear('timestamp')) \
        .withColumn('month', month('timestamp')) \
        .withColumn('year', year('timestamp')) \
        .withColumn('weekday', dayofweek('timestamp')) \
        .dropDuplicates()

    time_table \
        .write \
        .partitionBy('year', 'month') \
        .mode('overwrite') \
        .parquet(output_data+'time/')

    # Read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+'songs/')

    # Extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (song_df.title == df.song) & (song_df.duration == df.length))

    songplays_table = songplays_table.select(
        monotonically_increasing_id().alias('songplay_id'), 
        col('timestamp').alias('start_time'),
        col('userId').alias('user_id'),
        'level',
        'song_id',
        'artist_id',
        col('sessionId').alias('session_id'),
        'location',
        col('userAgent').alias('user_agent'))

    # Write songplays table to parquet files partitioned by year and month
    songplays_table.withColumn('year',year('start_time')) \
        .withColumn('month',month('start_time')) \
        .write \
        .partitionBy('year', 'month') \
        .mode('overwrite') \
        .parquet(output_data+'songplay/')
    


def main():
    '''
    The main function contains the values for input and output S3 paths
    Calls process_song_data and process_log_data methods
    '''
    spark = create_spark_session()
    input_data = "s3://udacity-dend/"
    output_data = "s3://sparkify-dlake/data/"
    
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
