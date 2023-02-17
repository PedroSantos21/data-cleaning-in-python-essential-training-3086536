# """
# Load traffic.csv into "traffic" table in sqlite3 database.

# Drop and report invalid rows.
# - ip should be valid IP(see ipaddress)
# - time must not be in the future
# - path can't be empty
# - status code must be a valid HTTP status code(see http.HTTPStatus)
# - size can't be negative or empty

# Report the percentage of bad rows. Fail the ETL if there are more than 5 % bad rows
# """

import pandas as pd
import sqlite3 as sql
from ipaddress import ip_address
from http import HTTPStatus
from contextlib import closing
from invoke import task

THRESHOLD_INVALID = 5


def data_validation(data):
    # ip should be valid IP(see ipaddress)
    try:
        ip_address(data['ip'])
    except ValueError as e1:
        return False

    # time must not be in the future
    present = pd.Timestamp.now()
    if data['time'] > present:
        print(f'Future date')
        return False

    # path can't be empty
    if pd.isnull(data['path']) or not data['path'].strip():
        print(f'Empty path')
        return False

    # status code must be a valid HTTP status code (see http.HTTPStatus)
    if data['status'] not in set(HTTPStatus):
        print(f'Invalid HTTP return')
        return False

    # size can't be negative or empty
    if pd.isnull(data['size']) or data['size'] < 0:
        print(f'Invalid Size')
        return False

    return True


def load_csv(path, date_cols=[]):
    df = pd.read_csv(path, parse_dates=date_cols)
    for date in date_cols:
        df[date] = pd.to_datetime(
            df[date], format="%Y-%m-%dT%H:%M:%SZ")
    return df


def db_conn(dataframe: pd.DataFrame, db='table'):
    db_name = db+'.db'
    with closing(sql.connect(db_name)) as conn:
        conn.execute('BEGIN')
        with conn:
            dataframe.to_sql(db, conn, index=False, if_exists=True)


# @task
def etl(csv_file):
    # load_csv(csv_file, date_cols=['time'])
    df = pd.read_csv(csv_file, parse_dates=['time'])

    valid_df = df[df.apply(data_validation, axis=1)]

    # Report the percentage of bad rows.
    non_valid_data = len(df) - len(valid_df)
    if non_valid_data > 0:
        percent_invalid = non_valid_data/len(df) * 100
        print(f'{percent_invalid:.2f}% invalid rows')

        # Fail the ETL if there are more than 5% bad rows
        if percent_invalid >= THRESHOLD_INVALID:
            raise ValueError(
                f'ETL Failed: Percentage of invalid date superior to {str(THRESHOLD_INVALID)}%')

    db_conn(valid_df, 'trafic')


if __name__ == '__main__':
    etl('Ch04/challenge/traffic.csv')

# %%
