import os
import glob
import json
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Every song file found is processed 
    - Returns the required data for tables songs and artists
    """
    # open song file
    df = pd.DataFrame(pd.read_json(filepath, lines=True))

    # insert song record
    song_data =  df[['song_id', 'title','artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name','artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Every song file found is processed 
    - Returns required information for tables time, users and songplays
    """
    # open log file
    df = pd.DataFrame(pd.read_json(filepath, lines=True))

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    t = df.ts
    
    # insert time data records
    time_data = [[0 for i in range(7)] for j in range(len(t))]
    #for every ts in t create a list to hold metadata
    x=0
    for i in t:
            ts = pd.Series(pd.Timestamp(i, unit='ms'))
            time_data[x][0] = ts.dt.values[0]
            time_data[x][1] = ts.dt.hour.values[0]
            time_data[x][2] = ts.dt.day.values[0]
            time_data[x][3] = ts.dt.week.values[0]
            time_data[x][4] = ts.dt.month.values[0]
            time_data[x][5] = ts.dt.year.values[0]
            time_data[x][6] = ts.dt.weekday.values[0]
            x=x+1
    column_labels = ['timestamp', 'hour', "day", 'week', 'month', 'year', 'weekday']
    
    #zip time_data into a dictionary and back to a dataframe
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, zip(*time_data))))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName','lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records\
    x=0
    for index, row in df.iterrows():
        #for x in range(len(t)):
            # get songid and artistid from song and artist tables
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()

            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None

            # insert songplay record
            df['artistid'], df['songid'], df['songplay_id'] = artistid, songid, index
            songplay_data =  df[['songplay_id', 'ts', 'userId', 'level', 'songid', 'artistid', 'sessionId', 'location', 'userAgent']].values[x].tolist() 
            cur.execute(songplay_table_insert, songplay_data)
            x=x+1

def process_data(cur, conn, filepath, func):
    """
    - searches through the given file path for any files within reach
    - echos the amount of file present and the status of processed files
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

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - main
    - makes initial connection to database and executes functions to process data
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()