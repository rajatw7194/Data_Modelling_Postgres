import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Function to insert records into songs and artists tables.
    
    Keyword arguments:
    cur argument - Uses the cursor object to execute insert statements.
    filepath argument - Follow the filepath and read the files present. Data in extracted and inserted into the tables.    
    """
    
    # open song file
    df_song = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df_song[['song_id','title','artist_id','year','duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df_song[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Function to insert records into users, time and songplays table.
    
    Keyword arguments:
    cur argument - Uses the cursor object to execute insert statements.
    filepath argument - Follow the filepath and read the files present. Data in extracted and inserted into the tables. 
    
    - processes log files in filepath
    """
    
    # open log file
    df_log = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df_next = df_log[df_log.page == 'NextSong'] 

    # convert timestamp column to datetime
    t = pd.to_datetime(df_next['ts'])
    
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.weekday, t.dt.month, t.dt.year]
    column_labels = ['timestamp','hour','day','week','weekday','month','year']
    dict_zip = dict(zip(column_labels,time_data))
    time_df = pd.DataFrame.from_dict(dict_zip)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df_log[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df_log.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Iterates selected process from the above two.
    
    cur argument - Uses the cursor object to execute insert statements.
    conn argument - Commit changes to the database.
    filepath argument - Follow the filepath and read the files present. Data in extracted and inserted into the tables. 
    func argument - The function that needs to be run

    - get all files matching extension from directory 
    
    - get total number of files found 
    
    - iterate over files and process
    
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
    - Establishes connection with the database and gets cursor.  
    
    - Run process_data function on song files 
    
    - Run process_data function on log files 
    
    - In the end, close the connection. 
    
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()