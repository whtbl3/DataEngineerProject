# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXITS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id varchar(32) NOT NULL,
        start_time timestamp NOT NULL,
        user_id INT4 NOT NULL,
        level VARCHAR(256),
        song_id VARCHAR(256),
        artist_id VARCHAR(256),
        session_id INT4,
        location VARCHAR(256),
        user_agent VARCHAR(256),
        CONSTRAINT songplays_pkey PRIMARY KEY (songplay_id)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT4 NOT NULL,
        first_name VARCHAR(256),
        last_name VARCHAR(256),
        gender VARCHAR(256),
        level VARCHAR(256),
        CONSTRAINT users_pkey PRIMARY KEY (user_id)
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR(256) NOT NULL,
        title VARCHAR(256),
        artist_id VARCHAR(256),
        year INT4,
        duration NUMERIC(18, 0),
        CONSTRAINT songs_pkey PRIMARY KEY (song_id)
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(256) NOT NULL,
        name VARCHAR(256),
        location VARCHAR(256),
        lattitude NUMERIC(18, 0),
        Longitude NUMERIC(18, 0),
        CONSTRAINT artists_pkey PRIMARY KEY (artist_id)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp NOT NULL,
        hour INT4,
        day INT4,
        week INT4,
        month VARCHAR(256),
        year INT4,
        weekday VARCHAR(256),
        CONSTRAINT time_pkey PRIMARY KEY (start_time)
    );
""")

# INSERT RECORDS

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
                       
""")


time_table_insert = ("""
""")

# FIND SONGS

song_select = ("""SELECT songs.song_id, artists.artist_id
                  FROM songs JOIN artists ON songs.artist_id=artists.artist_id
                  WHERE songs.title= %s
                  AND artists.name = %s
                  AND songs.duration = %s""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
