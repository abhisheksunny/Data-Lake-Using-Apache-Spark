import configparser
from datetime import datetime
import os
import sys 
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

runmode = "local"

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    # get filepath to song data file
    if (runmode == "local"):
        song_data_path = os.path.join(input_data,"song_data","[A-Z]*","[A-Z]*","[A-Z]*","*.json")
    else:
        song_data_path = os.path.join(input_data,"song-data","*","*","*","*.json")    
    
    # read song data file
    df = spark.read.json(song_data_path)

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite").parquet(os.path.join(output_data,"songs"))

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", "artist_name as name", "artist_location as location",
                                  "artist_latitude as lattitude", "artist_longitude as longitude").distinct()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(os.path.join(output_data,"artists"))
    
    print("INFO:> Song Data Processed.")

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    if (runmode == "local"):
        log_data_path = os.path.join(input_data,"log_data","*.json")
    else:
        log_data_path = os.path.join(input_data,"log-data","*","*","*.json")
    
    # read log data file
    df = spark.read.json(log_data_path)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.selectExpr("cast(userId as Int)as user_id", "firstName as first_name",
                                "lastName as last_name", "gender", "level").distinct()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(os.path.join(output_data,"users"))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(float(x)/1000.0), TimestampType())
    time_df = df.select(col("ts").alias("start_time")).distinct().withColumn("date_time",get_timestamp("start_time"))
    
    # extract columns to create time table
    time_table = time_df.select(
        "start_time", #"date_time",
        hour("date_time").alias("hour"),
        dayofmonth("date_time").alias("day"),
        weekofyear("date_time").alias("week"),
        month("date_time").alias("month"),
        year("date_time").alias("year"),
        date_format("date_time","F").alias("weekday")
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(os.path.join(output_data,"time"))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data,"songs"))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.alias('a').join( 
        song_df.alias('b'),
        col('b.title') == col('a.song'),
        "left_outer" 
    ).withColumn(
        "songplay_id", monotonically_increasing_id()
    ).select(
        "songplay_id",
        col("a.ts").alias("start_time"),
        col("a.userId").alias("user_id"),
        col("a.level").alias("level"),
        col("b.song_id").alias("song_id"),
        col("b.artist_id").alias("artist_id"),
        col("a.sessionId").alias("session_id"),
        col("a.location").alias("location"),
        col("a.userAgent").alias("user_agent")
    )

    songplays_table = songplays_table.join(
        time_table.select("start_time", "year", "month"),
        "start_time"
    ).select(
        "songplay_id", "start_time", "user_id", "level", "song_id", "artist_id",
        "session_id", "location", "user_agent", "year", "month"
    )
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet(os.path.join(output_data,"songplays"))
    
    print("INFO:> Log Data Processed.")

def main():

    if (len(sys.argv) == 2): runmode = sys.argv[1]
        
    if (runmode == "local"):
        input_data = "/home/workspace/data"
        output_data = "/home/workspace/data-output-test"
    elif (runmode == "S3"):
        input_data = "s3a://udacity-dend/"
        output_data = "s3a://udacity-data-lake-output/"
    else:
        print("Runmode not recognized. Available options: local | S3.")
        return
    print("INFO:> Running in {} mode.".format(runmode))
    
    spark = create_spark_session()
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
