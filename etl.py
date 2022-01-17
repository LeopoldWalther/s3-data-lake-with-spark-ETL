import configparser
from datetime import datetime
import os
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
    """ 
    Extract and transform song data and create songs and artists tables
    
        Args:
            spark {object}: SparkSession object
            input_data {object}: Source S3 bucket
            output_data {object}: Target S3 bucket
            
        Returns:
            None
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # read song data file
    df_song = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df_song.select('song_id', 'title', 'artist_id', 'year', 'duration').distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id') \
                        .mode('overwrite') \
                        .parquet(output_data + 'songs')

    # extract columns to create artists table
    artists_table = df_song.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').distinct()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite') \
                        .parquet(output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    """ Process log data, create users table, time table and songplays table

        Args:
            spark {object}: SparkSession object
            input_data {object}: Source S3 bucket
            output_data {object}: Target S3 bucket

        Returns:
            None
    """

    # get filepath to log data file
    log_data = os.path.join(input_data, "log-data/")

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where(df['page'] == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').distinct()

    # write users table to parquet files
    users_table.write.mode('overwrite') \
        .parquet(output_data + 'users')

    # create timestamp column from original timestamp column
    df = df.withColumn('start_time', from_unixtime((col('ts')/1000).cast('integer')).cast('timestamp'))
    df = df.withColumn('weekday', date_format(df['start_time'], 'E'))
    df = df.withColumn('year', year(df['start_time']))
    df = df.withColumn('month', month(df['start_time']))
    df = df.withColumn('week', weekofyear(df['start_time']))
    df = df.withColumn('day', dayofmonth(df['start_time']))
    df = df.withColumn('hour', hour(df['start_time']))

    # extract columns to create time table
    time_table = df.select('start_time', 'weekday', 'year', 'month', 'week', 'day', 'hour').distinct()

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite') \
                    .partitionBy('year', 'month') \
                    .parquet(path=output_data + 'time')

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    df_song = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(df_song, (df.song == df_song.title)\
                                        & (df.artist == df_song.artist_name)\
                                        & (df.length == df_song.duration), "inner") \
                            .distinct() \
                            .select('start_time', 'userId', 'level', 'song_id', \
                                    'artist_id', 'sessionId','location','userAgent', \
                                    df['year'].alias('year'), df['month'].alias('month')) \
                            .withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite") \
                            .partitionBy('year', 'month')\
                            .parquet(path=output_data + 'songplays')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
