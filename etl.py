import configparser
from datetime import datetime
from email import header
import os

from numpy import int64
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

#https://s3.console.aws.amazon.com/s3/buckets/udacity-dend/object/total_size?region=us-west-2&showversions=false

config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['default']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['default']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = 's3://udacity-dend/song_data/'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').option(header=True).partitionBy('year', 'artist_id').parquet(output_data+'songs/')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'name', 'location', 'lattitude', 'longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').option(header=True).parquet(output_data+'artists/')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = 's3://udacity-dend/log_data/'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter('page' == 'NextSong')

    df.createOrGetTempView('logs')
    # extract columns for users table    
    #user_table = df.select('user_id', 'first_name', 'last_name', 'gender', 'level')
    user_table = spark.sql('''
        SELECT distinct user_id, first_name, last_name, gender, level
        FROM logs
        ORDER BY ts DESC
    ''')

    # write users table to parquet files
    user_table.write.option(header=True).mode('overwrite').parquet('users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.utcfromtimestamp(int64(ts) // 1000000000)) 
    #year, month, dayofmonth, hour, weekofyear, date_format
    df = df.withColumn('timestamp', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda timestamp: date_format(timestamp, 'MM/dd/YYYY HH:mm:ss')) 

    # extract columns to create time table
    time_table = spark.sql('''
        SELECT 
            distinct timestamp as start_time, 
            EXTRACT(start_time, 'hour'), 
            EXTRACT(start_time, 'day'), 
            EXTRACT(start_time, 'week'), 
            EXTRACT(start_time, 'month'), 
            EXTRACT(start_time, 'year'), 
            EXTRACT(start_time, 'weekday')
        FROM logs
    ''')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.option(header=True).partitionBy('year', 'month').mode('overwrite').parquet()

    # read in song data to use for songplays table
    song_df = spark.read.parquet('songs')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = ''

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.option(header=True).partitionBy('year', 'month').mode('overwrite').parquet()


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://sparkify-dl/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
