## Background Information
The music streaming startup, Sparkify, has grown their user base and song database and currently have their data in S3, in a directory of containing files of JSON logs of user activity on the app, and another directory with JSON metadata about the songs on their app. 

## Purpose 
Converting song and log data files to properly formatted and segregated tables like structure in a Data Lake that would allow easy and instant query over the data for fetching meaningful results and insights. Apache Spark is used for data ingestion and ETL transformation. Amazon S3 is used for storing source JSON data and output parquet data. The python script file containing the Spark code runs on Amazon EMR, a cloud based computing instance modified for big-data use cases.
***

## Technology Stack
* Apache Spark
* AWS S3
* AWS EMR

***

## Running Instruction
Update `dl.cfg` file with AWS keys having required S3 permission.  
Update the variable output_data in `etl.py` with the destination data location.
Execute `python etl.py <MODE>` for transforming data from JSON to structured parquet files. Currently supported values for MODE are local and S3.

***

## Files
<ul>
    <li><b>dl.cfg -</b> Configuration file, containing AWS Keys.</li>
    <li><b>etl.py -</b> Python Script for transforming data.</li>
</ul>

***

## Data Ingestion
<b>Song File - </b> The data within these JSON files is read into a dataframe on-the-go and then is used to populate `songs` and `artists` tables.

<b>Log File - </b> Just as song data, Log File is loaded from JSON Files and all the remaining tables(`users`, `songplays`, `time`) are populated. 

## Additional Design Information

#### Fact Table
<b>songplays -</b> records in log data associated with song plays
+ songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    
        
#### Dimension Tables
<b>users -</b> users in the app
+ user_id, first_name, last_name, gender, level

<b>songs -</b> songs in music database
+ song_id, title, artist_id, year, duration

<b>artists -</b> artists in music database
+ artist_id, name, location, lattitude, longitude

<b>time -</b> timestamps of records in songplays broken down into specific units
+ start_time, hour, day, week, month, year, weekday

***