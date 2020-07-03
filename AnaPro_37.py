import pandas as pd
import os

from collections import namedtuple
from datetime import datetime as dt
import re
import numpy as np
from sqlalchemy import create_engine
from urllib import parse

# Setting constants
database_name = 'dateaubase2020'
local_server = r'GCI-PR-DATEAU02\DATEAUBASE'
remote_server = r'132.203.190.77\DATEAUBASE'
path = "//10.10.11.13/infpc1_2/"
with open('login.txt') as f:
    username = f.readline().strip()
    password = f.readline().strip()


def connect_local(server, database):
    engine = create_engine(f'mssql://{local_server}/{database}?driver=SQL+Server?trusted_connection=yes', connect_args={'connect_timeout': 2}, fast_executemany=True)
    return engine


def connect_remote(server, database, login_file):
    with open(login_file) as f:
        username = f.readline().strip()
        password = parse.quote_plus(f.readline().strip())
    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server', connect_args={'connect_timeout': 2}, fast_executemany=True)
    return engine


def get_last(db_engine):
    query = 'SELECT Value_ID, Timestamp FROM dbo.value WHERE Value_ID = (SELECT MAX(Value_ID) FROM dbo.value)'
    result = db_engine.execute(query)
    Record = namedtuple('record', result.keys())
    records = [Record(*r) for r in result.fetchall()]
    return records[0]

def engine_runs(engine):
    try:
        _ = get_last(engine)
    except Exception:
        return False
    else:
        return True

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
    df2 = pd.DataFrame(df.stack())
    df = df2
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
    '''stores df in SQL table dbo.value'''
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
engine = connect_local(local_server, database_name)
if engine_runs(engine):
    print('local connection engine is running')
else:
    print('local connection engine failed to connect. Trying remote')
    engine = connect_remote(remote_server, database_name, 'login.txt')
    if engine_runs(engine):
        print('remote connection engine is running')
    else:
        print('Remote connection engine failed to connect. Quitting.')

try:
    main(engine)
except Exception as e:
    print(e)
finally:
    engine.dispose()
