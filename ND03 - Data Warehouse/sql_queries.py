import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Truncate TABLES

staging_events_table_trun = "TRUNCATE TABLE staging_events"
staging_songs_table_trun = "TRUNCATE TABLE staging_songs"
songplay_table_trun = "TRUNCATE TABLE songplay"
user_table_trun = "TRUNCATE TABLE users"
song_table_trun = "TRUNCATE TABLE songs"
artist_table_trun = "TRUNCATE TABLE artists"
time_table_trun = "TRUNCATE TABLE time"

# DROP TABLES

staging_events_table_drop = "DROP TABLE staging_events"
staging_songs_table_drop = "DROP TABLE staging_songs"
songplay_table_drop = "DROP TABLE songplay"
user_table_drop = "DROP TABLE users"
song_table_drop = "DROP TABLE songs"
artist_table_drop = "DROP TABLE artists"
time_table_drop = "DROP TABLE time"

# CREATE TABLES


staging_events_table_create = (""" CREATE TABLE IF NOT EXISTS staging_events (  artist           VARCHAR, \
                                                                                auth             VARCHAR, \
                                                                                firstName        VARCHAR, \
                                                                                gender           VARCHAR, \
                                                                                itemInSession    VARCHAR, \
                                                                                lastName         VARCHAR, \
                                                                                length           FLOAT, \
                                                                                level            VARCHAR, \
                                                                                location         VARCHAR, \
                                                                                method           VARCHAR, \
                                                                                page             VARCHAR, \
                                                                                registration     VARCHAR, \
                                                                                sessionId        VARCHAR, \
                                                                                song             VARCHAR, \
                                                                                status           VARCHAR, \
                                                                                ts               VARCHAR, \
                                                                                userAgent        VARCHAR, \
                                                                                userId           VARCHAR ); """)

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs (num_songs            INTEGER, \
                                                                            artist_id            VARCHAR, \
                                                                            artist_latitude      VARCHAR, \
                                                                            artist_longitude     VARCHAR, \
                                                                            artist_location      VARCHAR, \
                                                                            artist_name          VARCHAR, \
                                                                            song_id              VARCHAR, \
                                                                            title                VARCHAR, \
                                                                            duration             FLOAT, \
                                                                            year                 INTEGER); """)

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplay  (songplay_id INTEGER IDENTITY(0,1) NOT NULL PRIMARY KEY, \
                                                                   start_time  DATE, \
                                                                   user_id     VARCHAR NOT NULL, \
                                                                   level       VARCHAR, \
                                                                   song_id     VARCHAR NOT NULL, \
                                                                   artist_id   VARCHAR NOT NULL, \
                                                                   session_id  VARCHAR, \
                                                                   location    VARCHAR, \
                                                                   user_agent  VARCHAR); """)

user_table_create = ("""  CREATE TABLE IF NOT EXISTS users (user_id   VARCHAR NOT NULL PRIMARY KEY, \
                                                           first_name VARCHAR, \
                                                           last_name  VARCHAR, \
                                                           gender     VARCHAR, \
                                                           level      VARCHAR); """)

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs (song_id   VARCHAR NOT NULL PRIMARY KEY, \
                                                           title     VARCHAR, \
                                                           artist_id VARCHAR NOT NULL, \
                                                           year      VARCHAR, \
                                                           duration  BIGINT  ); """)

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR NOT NULL PRIMARY KEY, \
                                                               name      VARCHAR, \
                                                               location  VARCHAR, \
                                                               latitude  VARCHAR, \
                                                               longitude VARCHAR) ; """)

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP NOT NULL PRIMARY KEY , \
                                                          hour        VARCHAR, \
                                                          day         VARCHAR, \
                                                          week        VARCHAR, \
                                                          month       VARCHAR, \
                                                          year        VARCHAR, \
                                                          weekday     VARCHAR); """)

# STAGING TABLES

staging_events_copy = (""" copy staging_events from {} \
                           credentials 'aws_iam_role={}'\
                           json {} \
                           region 'us-west-2';""").format(config.get("S3", "LOG_DATA"), config.get("IAM_ROLE", "ARN"),config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = (""" copy staging_songs from {} \
                          credentials 'aws_iam_role={}' \
                          json 'auto' \
                          region 'us-west-2';""").format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# Final Tables

songplay_table_insert = (""" INSERT INTO songplay (start_time, \
                                                   user_id, \
                                                   level, \
                                                   song_id, \
                                                   artist_id, \
                                                   session_id, \
                                                   location, \
                                                   user_agent) \
                             (SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second', \
                                    userId, \
                                    level, \
                                    song_id, \
                                    artist_id, \
                                    sessionId, \
                                    location, \
                                    useragent \
                              FROM   staging_songs, staging_events \
                              WHERE  title = song \
                              AND    artist = artist_name
                              AND    page = 'NextSong' ) """)

user_table_insert = (""" INSERT INTO users (user_id, \
                                            first_name, \
                                            last_name, \
                                            gender, \
                                            level) \
                                     (SELECT DISTINCT userId, \
                                             firstName, \
                                             lastname, \
                                             gender, \
                                             level \
                                     FROM    staging_events
                                     WHERE   page = 'NextSong') """)

song_table_insert = (""" INSERT INTO songs(song_id, \
                                           title, \
                                           artist_id, \
                                           year, \
                                           duration) \
                                         (SELECT DISTINCT song_id, \
                                                 title, \
                                                 artist_id, \
                                                 year, \
                                                 duration \
                                         FROM staging_songs) """)

artist_table_insert = (""" INSERT INTO artists (artist_id, \
                                                name, \
                                                location, \
                                                latitude, \
                                                longitude ) \
                                        (SELECT DISTINCT artist_id, \
                                                artist_name, \
                                                artist_location, \
                                                artist_latitude, \
                                                artist_longitude  \
                                        FROM staging_songs ) 
                       """)

time_table_insert = (""" INSERT INTO time (start_time,hour,day,week,month,year,weekday) \
                         ( SELECT  DISTINCT(start_time)                AS start_time, \
                                   EXTRACT(hour FROM start_time)       AS hour, \
                                   EXTRACT(day FROM start_time)        AS day, \
                                   EXTRACT(week FROM start_time)       AS week, \
                                   EXTRACT(month FROM start_time)      AS month, \
                                   EXTRACT(year FROM start_time)       AS year, \
                                   EXTRACT(dayofweek FROM start_time)  as weekday \
                                   FROM songplay);
                       """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, \
                        staging_songs_table_create, \
                        songplay_table_create, \
                        user_table_create, \
                        song_table_create, \
                        artist_table_create, \
                        time_table_create]

drop_table_queries = [staging_events_table_drop, \
                      staging_songs_table_drop, \
                      songplay_table_drop, \
                      user_table_drop, \
                      song_table_drop, \
                      artist_table_drop, \
                      time_table_drop]

copy_table_queries = [staging_events_copy, \
                      staging_songs_copy]

insert_table_queries = [songplay_table_insert, \
                        user_table_insert, \
                        song_table_insert, \
                        artist_table_insert, \
                        time_table_insert]

trunc_table_queries = [staging_events_table_trun, \
                       staging_songs_table_trun, \
                       songplay_table_trun, \
                       user_table_trun, \
                       song_table_trun, \
                       artist_table_trun, \
                       time_table_trun]
