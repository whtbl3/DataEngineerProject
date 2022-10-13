import os
import glob
import psycopg2
import pandas as pd
import StringIteratorIO
from sql_queries import *
from datetime import datetime
from typing import Any, Optional, Iterator, Dict

def clean_csv_file(value: Optional[Any]) -> str:
    """ Transforms a single value
        - Escape new lines: some of the text fields include newlines, so we escape \n -> \\n.
        - Empty values are transformed to \ N: default string used by PostgresSQL
            indicate NULL in copy 
    Args:
        value (Optional[Any]): string to transform

    Returns:
        str: string transformed
    """
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')

def copy_songs_iterator(cur: Any, songs: Iterator[Dict[str, Any]], entity: str) -> None:
    if entity == 'song':
        songs_string_iterator = StringIteratorIO((
            '|'.join(map(clean_csv_file, (
                song['song_id'],
                song['title'],
                song['artist_id'],
                song['year'],
                song['duration'],    
            ))) + '\n'
            for song in songs
        ))
        cur.copy_from(songs_string_iterator, 'songs', sep='|')
    elif entity == 'artist':
        artists_string_iterator = StringIteratorIO((
            '|'.join(map(clean_csv_file_file, (
               artist['artist_id'],
               artist['name'],
               artist['location'],
               artist['lattitude'],
               artist['longitude'] 
            ))) + '\n'
        ))
        cur.copy_from(songs_string_iterator, 'artists', sep='|')

def copy_log_iterator(cur: Any, songs: Iterator[Dict[str, Any]], entity: str) -> None:
    if entity == 'user':
        users_string_iterator = StringIteratorIO((
            '|'.join(map(clean_csv_file, (
                user['user_id'],
                user['first_name'],
                user['last_name'],
                user['gender'],
                user['level']
            ))) + '\n'
        ))
        cur.copy_from(user_table_create, 'users', sep='|')
    elif entity == 'time':
        time_string_iterator = StringIteratorIO((
            '|'.join(map(clean_csv_file, (
                time['start_time'],
                time['hour'],
                time['day'],
                time['week'],
                time['month'],
                time['weekday'],
            ))) + '\n'
        ))
        cur.copy_from(time_string_iterator, 'time', sep='|')
    
def process_song_file(cur: Any, filepath: str) -> None:
    """ Insert song, artist, into table
    Args:
        cur (Any): Any used to process large tables
        filepath (string): Path that contains the song file
    """
    # open song file
    song_file = open(filepath)
    song_data = json.loads(song_file.read())

    # insert song record
    copy_songs_iterator(cur, song_data, "song")
    
    # insert artist record
    copy_songs_iterator(cur, song_data, "artist")

def process_log_file(cur, filepath) -> None:
    """ Insert user, songplay, time into table

    Args:
        cur (Any): Any used to process large tables
        filepath (string): Path that contains the log file
    """
    # open log file
    log_df = pd.read_json(filepath, lines=True)
    log_df = log_df[time_df['NextSong']]

    # convert timestamp column to datetime
    log_df['ts'] = log_df['ts'].apply(lambda time: datetime.fromtimestamp(time / 1e3)) 
    
    # insert time data records
    t = log_df['ts'].apply(lambda time: datetime.fromtimestamp(time / 1e3))
    columns = ["timestamp", "hour", "day", "weekofyear", "month", "year", "dayofweek"]
    values = [log_df["ts"]] + [t.apply(lambda t: getattr(t, et)) for et in columns[1:]]
    datetime_df = pd.concat(values, keys=columns, axis=1)
    datetime_json = json.loads(dateime_df.to_json())
    copy_log_iterator(cur, datetime_json, "time")
    

    # load user table
    user_df = log_df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    
    # insert user records
    user_json = json.loads(user_df.to_json())
    copy_log_iterator(cur, datetime_json, "time")

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func) -> None:
    """ Process ETL

    Args:
        cur (Any): Any used to process large tables
        conn (connection): connection to the database
        filepath (string): file path to data to be processed
        func (Function): function that process datafile
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # # iterate over files and process
    # for i, datafile in enumerate(all_files, 1):
    #     func(cur, datafile)
    #     conn.commit()
    #     print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()