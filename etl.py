"""etl.py reads the data files and loads into the Redshit DB hosted in AWS"""
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

""" 
    Summary line. 
  
    process_song_data function reads the Json data from S3 and and processes it by extracting the data from song_data_table and writes to partitioned parquet files songs_table and artists_tables in table directories on S3
  
    Parameters: 
    spark       : Spark Session
    input_data  : S3 song_data location from where the data file is extracted
    output_data : S3 location where the parquent format data is written to
  
    Returns: 
    None
  
"""
def process_song_data(spark, input_data, output_data):

    song_data = input_data + 'song_data/*/*/*/*.json'
    
    df = spark.read.json(song_data)

    songs_table = spark.sql("""
                            SELECT 
                                song_id, 
                                title,
                                artist_id,
                                year,
                                duration
                            FROM 
                                song_data_table 
                            WHERE 
                                song_id IS NOT NULL
                        """)
    
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data+'songs_table/')
    
    artists_table = spark.sql("""
                                SELECT 
                                    DISTINCT artist_id, 
                                    artist_name,
                                    artist_location,
                                    artist_latitude,
                                    artist_longitude
                                FROM 
                                    song_data_table
                                WHERE 
                                    artist_id IS NOT NULL
                            """)
    
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/')

""" 
    Summary line. 
  
    process_log_data function reads the Json data from S3 and and processes it by extracting the data from log_data_tableand writes to partitioned parquet files users_table, time_table and songplays_table in table directories on S3
  
    Parameters: 
    cur (psycopg2.extensions.cursor): Contains the cursor information
    conn (string): Contains the connection information 
  
    Returns: 
    None
  
"""
def process_log_data(spark, input_data, output_data):

    log_data = input_data + 'log_data/*.json'

    df = spark.read.json(log_path)
    
    df = df.filter(df.page == 'NextSong')
    
    df.createOrReplaceTempView("log_data_table")

    users_table = spark.sql("""
                            SELECT 
                                DISTINCT userId, 
                                firstName,
                                lastName,
                                gender,
                                level
                            FROM 
                                log_data_table
                            WHERE 
                                userId IS NOT NULL
                        """)
    
    users_table.write.mode('overwrite').parquet(output_data+'users_table/')

    get_timestamp = udf(date_convert, TimestampType())
    df = df.withColumn("start_time", get_datetime('ts'))
    
    # extract columns to create time table
    time_table = spark.sql("""
                            SELECT 
                                A.start_time_sub as start_time,
                                hour(A.start_time_sub) as hour,
                                dayofmonth(A.start_time_sub) as day,
                                weekofyear(A.start_time_sub) as week,
                                month(A.start_time_sub) as month,
                                year(A.start_time_sub) as year,
                                dayofweek(A.start_time_sub) as weekday
                            FROM
                                (
                                 SELECT 
                                    to_timestamp(timeSt.ts/1000) as start_time_sub
                                 FROM 
                                     log_data_table
                                 WHERE ts IS NOT NULL
                                ) A
                        """)
    
     time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time_table/')

    song_df = spark.read.parquet(output_data+'songs_table/') 


    songplays_table = spark.sql("""
                                SELECT 
                                    monotonically_increasing_id() as songplay_id,
                                    to_timestamp(A.ts/1000)  as start_time,
                                    month(to_timestamp(A.ts/1000)) as month,
                                    year(to_timestamp(A.ts/1000)) as year,
                                    B.userId as user_id,
                                    B.level,
                                    B.song_id,
                                    B.artist_id,
                                    A.sessionId,
                                    A.location,
                                    A.userAgent as user_agent
                                FROM 
                                    log_data_table A
                                JOIN 
                                    song_data_table B 
                                ON 
                                    A.artist = B.artist_name 
                                    AND A.song = B.title
                            """)

    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays_table/')
    
 """ 
    Summary line. 
  
    main controls the ETL flow, it establishes the spark connection, sets the input and output path and and calls the functions process_song_data and process_log_data.
  
    Parameters: 
    None
  
    Returns: 
    None 
  
"""

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/dloutput/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
