import pandas as pd
import os

from collections import namedtuple
from datetime import datetime as dt
import re
import numpy as np
from sqlalchemy import create_engine

# Setting constants
database_name = 'dateaubaseSandbox'
local_server = r'GCI-PR-DATEAU01\DATEAUBASE'
remote_server = '10.10.10.10'
path = "//10.10.10.13/infpc1_2/"
with open('login.txt') as f:
    username = f.readline().strip()
    password = f.readline().strip()


def connect_local(server, database):
    try:
        engine = create_engine(f'mssql+pyodbc://@{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+11+for+SQL+Server')
    except Exception as e:
        print("local connection error")
        print(e)
        return None
    return engine


def connect_remote(server, database):
    try:
        engine = create_engine(f'mssql+pyodbc://jeandavidt:koopa6425@{remote_server}:1433/{database_name}?driver=ODBC+Driver+13+for+SQL+Server', fast_executemany=True)
        print('remote connection ok')
    except Exception as e:
        print("remote connection error")
        print(e)
        return None
    return engine


def get_last(db_engine):
    query = 'SELECT Value_ID, Timestamp FROM dbo.value WHERE Value_ID = (SELECT MAX(Value_ID) FROM dbo.value)'
    result = db_engine.execute(query)
    Record = namedtuple('record', result.keys())
    records = [Record(*r) for r in result.fetchall()]
    return records[0]


def get_par_files(path):
    full_path = os.path.join(os.getcwd(), path)

    file_list = []
    for file in os.listdir(full_path):
        if file.endswith(".par"):
            filename = os.path.join(full_path, file)
            file_list.append(filename)

    # Sort the list from the oldest to the last
    file_list.sort

    # Index of the oldest file with new data (will change during the execution but the last file always has new data)
    index = len(file_list) - 1
    return index, file_list


def read_par(f_name, db_engine):
    # load a file's data into a DataFrame
    df = pd.read_csv(f_name, sep='\t', skiprows=0, encoding='ANSI', header=1)

    # Rename and select only the columns you need
    new_cols = [
        'Datetime', 'Status', 'TSS', 'TSSinfo',
        'NO3N', 'NO3Ninfo', 'COD', 'CODinfo',
        'CODf', 'CODfinfo', 'NH4N', 'NH4Ninfo',
        'K', 'Kinfo', 'pH', 'pHinfo',
        'Temp', 'Tempinfo'
    ]
    df.columns = new_cols
    cols_to_keep = ['Datetime', 'TSS', 'NO3N', 'COD', 'CODf', 'NH4N', 'K', 'pH', 'Temp']
    df = df[cols_to_keep]

    # Fill the 'NaN' values
    df.fillna(0, inplace=True)

    # Convert the datetime strings into epoch time
    df['Datetime'] = pd.to_datetime(df['Datetime'], format='%Y.%m.%d  %H:%M:%S').dt.tz_localize('US/Eastern').astype(np.int64) // 10 ** 9

    # Get the last ID from the database
    last_ID, last_Timestamp = get_last(engine)

    # Remove rows with a timestamp already in the dateaubase
    time_mask = (df['Datetime'] > last_Timestamp)
    df = df[time_mask]
    if (not len(df)):
        return None
    # Stack the values into unique records
    df.set_index('Datetime', inplace=True)
    df = pd.DataFrame(df.stack())
    df.reset_index(inplace=True)

    # Rename the columns
    df.columns = ['Timestamp', 'Metadata_ID', 'Value']

    # Map the parameter names to the correct metadata_id
    mapping = {
        'TSS': 5,
        'NO3N': 6,
        'COD': 7,
        'CODf': 8,
        'NH4N': 1,
        'K': 2,
        'pH': 3,
        'Temp': 4
    }
    df['Metadata_ID'] = df['Metadata_ID'].map(mapping)

    # Apply a factor of 1000 for rows where Metadata_ID is 5, 6, 7 or 8
    # These contain values for TSS, NO3, COD and CODf respectively
    mask = (df['Metadata_ID'] > 4)
    df.loc[mask, 'Value'] = df['Value'] / 1000

    # Add 'Number of experiments column
    df['Number_of_experiment'] = 1
    df['Comment_ID'] = np.nan

    # Add the lastID value plus increment to the index of the new values
    df.index = df.index + last_ID + 1

    # Turn index into regular column
    df.reset_index(inplace=True)
    df.rename(columns={
        'index': 'Value_ID'
    }, inplace=True)

    # reorder columns
    df = df[['Value_ID', 'Value', 'Number_of_experiment', 'Metadata_ID', 'Comment_ID', 'Timestamp']]

    # And return result!
    return df


def send_to_db(df, db_engine):
    # stores df in SQL table dbo.value
    df.to_sql('value', con=db_engine, if_exists='append', index=False)
    return None


# _________Main Function__________


def main(engine):
    # Find the .par files on the path
    index, file_list = get_par_files(path)

    # get the latest timestamp on the dateaubase
    _, last_Timestamp = get_last(engine)

    # Convert the Timestamp in DateTime format
    db_last_time = dt.fromtimestamp(last_Timestamp)

    # loop to check all the files for data more recent than the last value in the db
    for idx, file in enumerate(file_list):
        with open(file, 'r') as f:
            first_point_line = f.read().splitlines()[2]
            first_time_string = re.split(r'\t', first_point_line)[0]
            file_first_time = dt.strptime(first_time_string, '%Y.%m.%d  %H:%M:%S')
        # if a file is more recent than the last data => change the index to begin the import with the
        # file just before this one
        if file_first_time > db_last_time:
            index = max(idx - 1, 0)
            break

    new_records = 0
    for file in file_list[index:]:
        new_data = read_par(file, engine)
        if new_data is not None:
            send_to_db(new_data, engine)
            new_records += len(new_data)

    print(f"Added {new_records} rows to {database_name}")


# ________Main Script_________
try:
    engine = connect_local(local_server, database_name)
    if engine is not None:
        print('local connection engine is running')
except Exception:
    print(e)

'''try:
    engine = connect_remote(remote_server, database_name)
    print('remote connection engine is running')
except Exception as e:
    print(e)'''
if engine is not None:
    try:
        main(engine)
    finally:
        engine.dispose()
