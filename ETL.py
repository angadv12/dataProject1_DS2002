import requests
import csv
import json
import pandas as pd
import sqlite3


"""
1) fetch / download / retrieve data
    - url
    - local file

args: source - str
returns: str of source content
"""
def fetch_data(source):
    if source.startswith("http"):
        # if url
        response = requests.get(source)
        return response.text
    else:
        # if local file
        with open(source, 'r') as file:
            return file.read()


"""
2) detect format of file and convert data format
    - input: csv or json or sql table
    - output: file format of user choice
"""
# TODO


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
    print(raw_data)


    return;


if __name__ == "__main__":
    process_data("cville_residential.csv")
    process_data("https://api.github.com/users/angadv12/repos")