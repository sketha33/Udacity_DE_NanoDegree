import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def truncate_table(cur, conn):
    """
    @Truncates each table using the queries in `drop_table_queries` list.
    """
    drop_table_queries = ['TRUNCATE TABLE songplays;', 'TRUNCATE TABLE users;', 'TRUNCATE TABLE songs;', 'TRUNCATE TABLE artists;', 'TRUNCATE TABLE time;']
    for query in drop_table_queries:
        try:
            cur.execute(query)
        except:
            print("Error when Truncating the table - " + query)
        conn.commit()

    

def process_song_file(cur, filepath):
    """
    @param cur: This is a cur parameter to perform DB DML operations. 
    @param filepath: This is input file to be read
    @return: No return values for the function
    @Description: This function reads the songs data from the /data/song_data directory and loads data into songs and artists table.
    """

    # open song file
    df = pd.read_json(path_or_buf=filepath, lines=True)
    
    # insert song record
    song_data = df[["song_id","artist_id","title","year","duration"]].values[0].tolist()
    #print(song_data)
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data =  df[["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):

    """
    @param cur: This is a cur parameter to perform DB DML operations. 
    @param filepath: This is input file to be read. 
    @return: No return values for the function
    @Description: This function reads the songs data from the /data/log_data directory loads the data into time, users, 
    """

    # open log file
    df = pd.read_json(path_or_buf=filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = ([tim, tim.hour, tim.day, tim.week, tim.month, tim.year, tim.weekday()] for tim in t)
    column_labels = (['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'])
    time_df = pd.DataFrame(time_data,columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId","firstName","lastName","gender","level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
  
        # get songid and artistid from song and artist tables
    
        #if row.song == 'Setanta matins' and row.artist == 'Elena':
        #    print(row.length)
        
        cur.execute(song_select, (row.song, row.artist, row.length))
        #cur.execute(song_select, (row.song, row.artist))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
            print(songid+ ',' + artistid + ',' + row.song + ',' + row.artist)
        else:
            songid, artistid = None, None
        
        # insert songplay record
        songplay_data = [pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    #print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        #print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    truncate_table(cur, conn)
    print("Table Truncate Complete!!!")
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    conn.close()

if __name__ == "__main__":
    main()