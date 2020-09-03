import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg', encoding='utf-8-sig')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function creates our Spark session and returns a Spark object.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function processes the song data from S3, creates the appropriate tables, 
    converts them to parquet format and moves them to a new S3 bucket.
    """
    # get filepath to song data file
    song_data = os.path.join(input_data,"song_data/A/A/A/*.json")
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(os.path.join(output_data, "songs"))

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data, "artists"))


def process_log_data(spark, input_data, output_data):
    """
    This function processes the log data from S3, creates the appropriate tables, 
    converts them to parquet format and moves them to a new S3 bucket.
    """
    # get filepath to log data file
    log_data = "s3a://udacity-dend/log-data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level')
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data, "users"))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:  datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    #get_timestamp = udf(lambda x: x/1000, IntegerType())
    df = df.withColumn('start_time', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    #get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    #get_datetime = udf(lambda x: from_unixtime(x), TimestampType())
    #df = df.withColumn('datetime', get_datetime('start_time'))
    
    # extract columns to create time table
    time_table = df.selectExpr('start_time',
                               'hour(start_time) as hour',
                               'dayofmonth(start_time) as day',
                               'weekofyear(start_time) as week',
                               'month(start_time) as month',
                               'year(start_time) as year',
                               'dayofweek(start_time) as weekday'
                              ).distinct().orderBy('start_time')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time'), 'overwrite')

    # read in song data to use for songplays table
    songs_data = os.path.join(input_data, "song_data/A/A/A/*.json")
    song_df = spark.read.json(songs_data)
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.artist == song_df.artist_name) & (df.length == song_df.duration), 'left_outer')\
        .select(
            df.start_time,
            col("userId").alias('user_id'),
            df.level,
            song_df.song_id,
            song_df.artist_id,
            col("sessionId").alias("session_id"),
            df.location,
            col("useragent").alias("user_agent"),
            year('start_time').alias('year'),
            month('start_time').alias('month')
        )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet(os.path.join(output_data, "songplays"))
                    

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://aws-logs-259860842210-us-west-2/elasticmapreduce/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
