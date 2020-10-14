import findspark
findspark.init()

import configparser
from datetime import datetime
import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, from_unixtime, floor, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():

    """
    This function creates a spark session, return spark context object.
    This object would be perform all the operations in spark.
    """

    spark = SparkSession. builder.\
        config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"). \
        config("spark.sql.session.timeZone", "America/Los_Angeles").\
        getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function reads the json files stored on the s3 bucket and extracts data from songs and artists tables.
    Parquet files are created in songs and artist folder.

    Input Parameter
        spark : Spark Session Object
        input_data : Spark object reads data from this location and create a dataframe object.
        output_data:All the output files are stored in this location
    """

    # get filepath to song data file
    input_data = input_data+"/song-data"
    print(input_data)
    
    """
    for x in os.walk(input_data):
        for y in glob.glob(os.path.join(x[0], '*.json')):
            song_data.append(y)
    """

    df = spark.read.json(input_data)
    df.createOrReplaceTempView("Staging_Song_Data")
    song_Data_DF = spark.sql("select * from Staging_Song_Data")

    # extract columns to create songs table
    songs_query = " SELECT song_id, title, artist_id, year, duration " \
                  " FROM Staging_Song_Data"
    songs_table = spark.sql(songs_query)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year","artist_id").parquet(path=output_data+"songs")

    # extract columns to create artists table
    artists_query="select artist_id, artist_name as name, " \
                  "       artist_location as location, " \
                  "       artist_latitude as latitude, " \
                  "       artist_longitude as longitude " \
                  "from Staging_Song_Data "
    artists_table =spark.sql(artists_query)
    artists_table = artists_table.dropDuplicates(['artist_id'])

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(path=output_data+"artists")

def process_log_data(spark, input_data, output_data):

    """
    This function reads the json files (log data) stored on the s3 bucket and extracts data from users, time and songsplays tables.
    Parquet files are created in songs and artist folder.

    Input Parameter
        spark : Spark Session Object
        input_data : Spark object reads data from this location and create a dataframe object.
        output_data:All the output files are stored in this location
    """

    # get filepath to log data file
    input_data = input_data+"/log-data"

    """
    log_data=[]
    for x in os.walk(input_data):
        for y in glob.glob(os.path.join(x[0], '*.json')):
            log_data.append(y)
    """
    
    # read log data file
    df = spark.read.json(input_data)

    # filter by actions for song plays
    df=df.filter(col("page")=='NextSong').withColumn("new_ts", df["ts"].cast(IntegerType())).drop("ts").withColumnRenamed("new_ts", "ts")
    df.createOrReplaceTempView("staging_log_data")

    # extract columns for users table
    user_query = " SELECT userid, firstName, lastName, gender, level " \
                 " FROM staging_log_data "
    users_table = spark.sql(user_query)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(path=output_data+"users")

    df=df.filter(df['ts'].isNotNull())
        time_table= df.select(from_unixtime(df['ts']/1000).alias('start_time'))
    time_table=time_table.select(time_table['start_time'], \
                                  hour(time_table['start_time']).alias("hour"), \
                                  dayofmonth(time_table['start_time']).alias("day"), \
                                  weekofyear(time_table['start_time']).alias("week"), \
                                  month(time_table['start_time']).alias("month"), \
                                  year(time_table['start_time']).alias("year"), \
                                  date_format(time_table['start_time'],'E').alias("DOW"))

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year','month').parquet(path=output_data + "time")

    # read in song data to use for songplays table
    songplay_query=" Select DISTINCT monotonically_increasing_id() as songplay_id, " \
                   "        from_unixtime(ld.ts/1000)  as start_time , " \
                   "        ld.userid as user_id, " \
                   "        ld.level as level,"\
                   "        sd.song_id as song_id," \
                   "        sd.artist_id as artist_id," \
                   "        ld.sessionid as  session_id, " \
                   "        ld.location as location, " \
                   "        ld.useragent as user_agent, " \
                   "        t.year as year, " \
                   "        t.month as month " \
                   " from staging_log_data ld, Staging_Song_Data sd, time t" \
                   " Where ld.artist = sd.artist_name" \
                   "   and ld.song = sd.title " \
                   "   and from_unixtime(ld.ts/1000) = t.start_time " 

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql(songplay_query)
    #songplays_table = spark.sql(songplay_query).drop_duplicates('start_time','user_id','level','song_id','artist_id','location','user_agent')

    songplays_table.show()
  
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year','month').parquet(path=output_data + "songplays")

def main():
    spark = create_spark_session()

    input_data  = "s3a://udacity-dend"
    output_data = "s3a://udacity-nd-dend-bucket"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()