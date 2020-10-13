import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    drop_table_queries = ['drop TABLE IF EXISTS songplays;', \
                          'drop TABLE IF EXISTS users;', \
                          'drop TABLE IF EXISTS songs;', \
                          'drop TABLE IF EXISTS artists;', \
                          'drop TABLE IF EXISTS time;']
    for query in drop_table_queries:
        try:
            print(query)
            cur.execute(query)
        except:
            print("Error when deleting the table - " + query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    create_table_queries = ['CREATE TABLE IF NOT EXISTS songplays (songplay_id SERIAL NOT NULL PRIMARY KEY, \
                                                                   start_time date, \
                                                                   user_id varchar NOT NULL, \
                                                                   level varchar, \
                                                                   song_id varchar, \
                                                                   artist_id varchar, \
                                                                   session_id varchar, \
                                                                   location varchar, \
                                                                   user_agent varchar)' 
                            ,'CREATE TABLE IF NOT EXISTS users (user_id varchar NOT NULL PRIMARY KEY, \
                                                                first_name varchar, \
                                                                last_name varchar, \
                                                                gender varchar, \
                                                                level varchar)'
                            ,'CREATE TABLE IF NOT EXISTS songs (song_id    varchar NOT NULL PRIMARY KEY, \
                                                                title      varchar, \
                                                                artist_id  varchar NOT NULL, \
                                                                year       varchar, \
                                                                duration   bigint) ' \
                            ,'CREATE TABLE IF NOT EXISTS artists (artist_id varchar NOT NULL PRIMARY KEY, \
                                                                  name      varchar, \
                                                                  location  varchar, \
                                                                  latitude  varchar, \
                                                                  longitude varchar)' 
                            ,'CREATE TABLE IF NOT EXISTS time (start_time timestamp NOT NULL PRIMARY KEY , \
                                                               hour    varchar, \
                                                               day     varchar, \
                                                               week    varchar, \
                                                               month   varchar, \
                                                               year    varchar, \
                                                               weekday varchar)']
                  

    #print(create_table_queries)
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()