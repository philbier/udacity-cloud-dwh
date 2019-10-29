import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
AWS_KEY=config['AWS']['ACCESS_KEY']
AWS_SECRET=config['AWS']['SECRET_KEY']
SONG_DATA=config['S3']['SONG_DATA']
LOG_DATA=config['S3']['LOG_DATA']
LOG_JSONPATH=config['S3']['LOG_JSONPATH']


# DROP TABLES
staging_events_table_drop = "drop table if exists public.staging_events"
staging_songs_table_drop = "drop table if exists public.staging_songs"
songplay_table_drop = "drop table if exists public.songplays"
user_table_drop = "drop table if exists public.users"
song_table_drop = "drop table if exists public.songs"
artist_table_drop = "drop table if exists public.artists"
time_table_drop = "drop table if exists public.times"


# CREATE TABLES
## all foreign keys should have NOT NULL Constraint
staging_events_table_create= ("""
CREATE TABLE public.staging_events (
	artist varchar(256),
	auth varchar(256),
	firstname varchar(256),
	gender varchar(256),
	iteminsession int4,
	lastname varchar(256),
	length numeric(18,0),
	"level" varchar(256),
	location varchar(256),
	"method" varchar(256),
	page varchar(256),
	registration numeric(18,0),
	sessionid int4,
	song varchar(256),
	status int4,
	ts int8,
	useragent varchar(256),
	userid int4
);
""")

staging_songs_table_create = ("""
CREATE TABLE public.staging_songs (
	num_songs int4,
	artist_id varchar(256),
	artist_name varchar(256),
	artist_latitude numeric(18,0),
	artist_longitude numeric(18,0),
	artist_location varchar(256),
	song_id varchar(256),
	title varchar(256),
	duration numeric(18,0),
	"year" int4
);
""")

songplay_table_create = ("""
CREATE TABLE public.songplays (
	playid varchar(32) NOT NULL,
	start_time timestamp NOT NULL,
	userid int4 NOT NULL,
	"level" varchar(256),
	songid varchar(256) NOT NULL,
	artistid varchar(256) NOT NULL,
	sessionid int4,
	location varchar(256),
	user_agent varchar(256),
	CONSTRAINT songplays_pkey PRIMARY KEY (playid)
);
""")

user_table_create = ("""
CREATE TABLE public.users (
	userid int4 NOT NULL,
	first_name varchar(256),
	last_name varchar(256),
	gender varchar(256),
	"level" varchar(256),
	CONSTRAINT users_pkey PRIMARY KEY (userid)
);
""")

song_table_create = ("""
CREATE TABLE public.songs (
	songid varchar(256) NOT NULL,
	title varchar(256),
	artistid varchar(256) NOT NULL,
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (songid)
);
""")

artist_table_create = ("""
CREATE TABLE public.artists (
	artistid varchar(256) NOT NULL,
	name varchar(256),
	location varchar(256),
	lattitude numeric(18,0),
	longitude numeric(18,0)
);
""")

time_table_create = ("""
CREATE TABLE public.times (
	start_time datetime NOT NULL,
	"hour" int4,
	"day" int4,
	"week" int4,
	"month" int4,
	"year" int4,
	"dayofweek" int4,
	CONSTRAINT time_pkey PRIMARY KEY (start_time)
);
""")

# TRUNCATE TABLES
staging_events_table_truncate = ("""
TRUNCATE TABLE public.staging_events
""")

staging_songs_table_truncate = ("""
TRUNCATE TABLE public.staging_songs
""")

songplay_table_truncate = ("""
TRUNCATE TABLE public.songplays
""")

user_table_truncate = ("""
TRUNCATE TABLE public.users
""")

artist_table_truncate = ("""
TRUNCATE TABLE public.artists
""")

song_table_truncate = ("""
TRUNCATE TABLE public.songs
""")

time_table_truncate = ("""
TRUNCATE TABLE public.times
""")

# STAGING TABLES
staging_events_copy = ("""
COPY {}
FROM {}
ACCESS_KEY_ID '{}'
SECRET_ACCESS_KEY '{}'
JSON {}
COMPUPDATE OFF
""").format("public.staging_events", LOG_DATA, AWS_KEY, AWS_SECRET, LOG_JSONPATH)

staging_songs_copy = ("""
COPY {}
FROM {}
ACCESS_KEY_ID '{}'
SECRET_ACCESS_KEY '{}'
JSON 'auto'
COMPUPDATE OFF
""").format("public.staging_songs", SONG_DATA, AWS_KEY, AWS_SECRET)

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO public.songplays
 (SELECT
     md5(events.sessionid || events.start_time) songplay_id,
     events.start_time, 
     events.userid, 
     events.level, 
     songs.song_id, 
     songs.artist_id, 
     events.sessionid, 
     events.location, 
     events.useragent
 FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
 FROM public.staging_events
 WHERE page='NextSong') events
 LEFT JOIN public.staging_songs songs
 ON events.song = songs.title
 AND events.artist = songs.artist_name
 AND events.length = songs.duration)
""")

user_table_insert = ("""
INSERT INTO public.users
        (SELECT distinct userid, firstname, lastname, gender, level
        FROM public.staging_events
        WHERE page='NextSong')
""")

song_table_insert = ("""
INSERT INTO public.songs
(SELECT distinct song_id, title, artist_id, year, duration
FROM public.staging_songs)
""")

artist_table_insert = ("""
INSERT INTO public.artists
(SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM public.staging_songs)
""")

time_table_insert = ("""
INSERT INTO public.times
(SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
    extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
FROM public.songplays)
""")


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
truncate_table_queries = [staging_events_table_truncate, staging_songs_table_truncate, user_table_truncate, artist_table_truncate, song_table_truncate, time_table_truncate]
copy_table_queries = [staging_events_copy, staging_songs_copy, songplay_table_truncate]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
