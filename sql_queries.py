import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ENDPOINT = config.get("CLUSTER","HOST")
DWH_ROLE_ARN = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs_talbe"
songplay_table_drop = "DROP table IF EXISTS songplay_table"
user_table_drop = "DROP table IF EXISTS user_table"
song_table_drop = "DROP table IF EXISTS song_table"
artist_table_drop = "DROP table IF EXISTS artist_table"
time_table_drop = "DROP table IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events_table (
artist varchar,
auth varchar,
firstName varchar,
gender char,
iteminSession int,
lastName varchar,
length numeric,
level varchar,
location varchar,
method varchar,
page varchar,
registration numeric,
sessionId numeric,
song varchar,
status numeric,
ts numeric PRIMARY KEY,
userAgent varchar,
userId NUMERIC
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_table (
num_songs int,
artist_id varchar,
artist_latitude varchar,
artist_longitude varchar,
artist_location varchar,
artist_name varchar,
song_id varchar PRIMARY KEY,
title varchar,
duration numeric,
year int
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table (
songplay_id int IDENTITY(0,1) PRIMARY KEY, 
start_time TIMESTAMP NOT NULL, 
user_id NUMERIC NOT NULL, 
level varchar, 
song_id varchar NOT NULL, 
artist_id varchar NOT NULL, 
session_id NUMERIC NOT NULL, 
location varchar, 
user_agent varchar NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table (
user_id NUMERIC UNIQUE NOT NULL PRIMARY KEY, 
first_name varchar NOT NULL, 
last_name varchar NOT NULL, 
gender char, 
level varchar
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table (
song_id varchar PRIMARY KEY, 
title varchar NOT NULL, 
artist_id varchar NOT NULL, 
year NUMERIC, 
duration numeric NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table (
artist_id varchar PRIMARY KEY, 
artist_name varchar NOT NULL, 
artist_location varchar, 
artist_latitude varchar, 
artist_longitude varchar
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table (
start_time TIMESTAMP PRIMARY KEY, 
hour numeric NOT NULL, 
day int NOT NULL, 
week int NOT NULL, 
month int NOT NULL, 
year int NOT NULL, 
weekday int NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events_table FROM 's3://udacity-dend/log_data' 
credentials 
'aws_iam_role={}'
region 'us-west-2'
JSON 's3://udacity-dend/log_json_path.json' TRUNCATECOLUMNS;
""").format(DWH_ROLE_ARN)

staging_songs_copy = ("""
COPY staging_songs_table FROM 's3://udacity-dend/song_data' 
credentials 
'aws_iam_role={}'
region 'us-west-2'
JSON 'auto' TRUNCATECOLUMNS;
""").format(DWH_ROLE_ARN)


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay_table 
(start_time, user_id, level, song_id,
 artist_id, session_id, location, user_agent) 
(SELECT TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' AS start_time,
 e.userid, e.level, s.song_id,
 s.artist_id, e.sessionid,
 e.location, e.useragent
FROM staging_events_table AS e
JOIN staging_songs_table AS s
ON e.song = s.title
WHERE e.page = 'NextSong')
""")

user_table_insert = ("""
INSERT INTO user_table 
(user_id , first_name, last_name, 
gender, level)
(SELECT DISTINCT e.userid, e.firstname, e.lastname, e.gender, e.level
FROM staging_events_table AS e
WHERE e.userid IS NOT NULL
AND e.firstname IS NOT NULL)
""")

song_table_insert = ("""
INSERT INTO song_table (
song_id,title,artist_id,year,duration) 
(SELECT s.song_id, s.title, s.artist_id, s.year, s.duration
FROM staging_songs_table AS s
WHERE s.song_id IS NOT NULL)
""")

artist_table_insert = ("""
INSERT INTO artist_table (
artist_id,artist_name,artist_location,
artist_latitude,artist_longitude) 
(SELECT DISTINCT s.artist_id, s.artist_name AS name,
 s.artist_location AS location, s.artist_latitude AS latitude,
 s.artist_longitude AS longitude
FROM staging_songs_table AS s
WHERE s.artist_id IS NOT NULL)
""")

time_table_insert = ("""
INSERT INTO time_table (
start_time, hour, day, 
week, month, year, weekday) 
(SELECT start_time, 
 DATE_PART(h, start_time) AS hour,
 DATE_PART(d, start_time) AS day,
 DATE_PART(w, start_time) AS week,
 DATE_PART(mon, start_time) AS month,
 DATE_PART(y, start_time) AS year,
 DATE_PART(dayofweek, start_time) AS weekday
FROM songplay_table)
""")

# Show 5 entires for each table created

show_songplay = ("""SELECT * FROM songplay_table LIMIT 5""")
show_users = ("""SELECT * FROM user_table LIMIT 5""")
show_songs = ("""SELECT * FROM song_table LIMIT 5""")
show_artists = ("""SELECT * FROM artist_table LIMIT 5""")
show_time = ("""SELECT * FROM time_table LIMIT 5""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
show_table_queries = [show_songplay, show_users, show_songs, show_artists, show_time]
