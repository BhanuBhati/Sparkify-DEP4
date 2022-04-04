import configparser
from datetime import datetime
from email import header
import os

from numpy import int64
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = 's3://udacity-dend/song_data'
    
    # read song data file
    df = spark.read.json(song_data)
    '''
    {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
    song_id, title, artist_id, year, duration
    '''

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.option(header=True).partitionBy('year', 'artist_id').mode('overwrite').parquet()

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'name', 'location', 'lattitude', 'longitude')
    
    # write artists table to parquet files
    artists_table.write.option(header=True).mode('overwrite').parquet()


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = 's3://udacity-dend/log_data'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter('page' == 'NextSOng')

    # extract columns for users table    
    user_table = df.select('user_id', 'first_name', 'last_name', 'gender', 'level')
    
    # write users table to parquet files
    user_table.write.option(header=True).mode('overwrite').parquet()

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.utcfromtimestamp(int64(ts) // 1000000000)) 
    df = df.withColumn('timestamp', get_datetime('ts'))
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.option(header=True).partitionBy('year', 'month').mode('overwrite').parquet()

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.option(header=True).partitionBy('year', 'month').mode('overwrite').parquet()


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
