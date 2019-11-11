import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    # get filepath to song data file
    song_data_path = os.path.join(input_data,"song_data","[A-Z]*","[A-Z]*","[A-Z]*","*.json")
    
    # read song data file
    df = spark.read.json(song_data_path)

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").parquet(os.path.join(output_data,"songs"))

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", "artist_name as name", "artist_location as location",
                                  "artist_latitude as lattitude", "artist_longitude as longitude")
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(os.path.join(output_data,"artists"))
    
    print("** Song Data Processed!")

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data_path = os.path.join(input_data,"log_data","*.json")

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
    time_df = df.select(col("ts").alias("start_time")).withColumn("date_time",get_timestamp("start_time"))
    
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
    time_table.write.mode("overwrite").parquet(os.path.join(output_data,"time"))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data,"songs"))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.alias('a').join( 
        song_df.alias('b'),
        col('b.title') == col('a.song'),
        "left_outer" 
    ).select(
        col("a.ts").alias("start_time"),
        col("a.userId").alias("user_id"),
        col("a.level").alias("level"),
        col("b.song_id").alias("song_id"),
        col("b.artist_id").alias("artist_id"),
        col("a.sessionId").alias("session_id"),
        col("a.location").alias("location"),
        col("a.userAgent").alias("user_agent")
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").parquet(os.path.join(output_data,"songplays"))
    
    print("** Log Data Processed!")

def main():
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    input_data = "/home/workspace/data"
    output_data = "/home/workspace/data-output-test"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
