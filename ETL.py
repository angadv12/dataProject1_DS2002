import requests
import csv
import json
import pandas as pd
from io import StringIO
import sqlite3


"""
1) fetch / download / retrieve data
    - url
    - local file

args: source - str
returns: str of source content
"""
def fetch_data(source):
    try:
        if source.startswith("http"):
            # if url
            response = requests.get(source)
            response.raise_for_status() # raises HTTP error for bad responses
            return response.text
        else:
            # if local file
            with open(source, 'r') as file:
                return file.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {source}")
    except requests.exceptions.RequestException as e:
        raise requests.exceptions.RequestException(f"Failed to fetch data from URL: {source}. Error: {str(e)}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")
    return;


"""
2) detect format of file and convert data format
    - input: csv or json files
    - output: file in format of user choice (csv, json, sql)
"""
def detect_file_format(data):
    try:
        json.loads(data)
        return 'json' # if json load successful, then is json format
    except:
        try:
            pd.read_csv(StringIO(data))
            return 'csv' # if pandas read csv succesful, then is csv format
        except:
            raise ValueError("Data format not recognized (must be JSON or CSV).")


"""
3) modify number of columns of data
    - reduce or add cols
    - for add you can put any info you want
"""
# TODO


"""
4) store new file
    - either to local files or sql database
"""
# TODO


"""
5) generate summary of data file ingestion
    - include: num records, num cols
"""
# TODO


"""
6) generate summary of post processing
    - include: num records, num cols
"""
# TODO


# processor function
def process_data(source):

    # fetch data
    print("Fetching data...")
    raw_data = fetch_data(source)

    # check format
    format = detect_file_format(raw_data)
    print(f"File format: {format}")

    return;


if __name__ == "__main__":
    process_data("cville_residential.csv")
    process_data("https://api.github.com/users/angadv12/repos")