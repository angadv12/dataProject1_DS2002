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

def convert_data_format(data, source_format, target_format, database_name=None, table_name=None):
    if source_format == target_format:
        return data
    
    # CSV to JSON
    if source_format == 'csv' and target_format == 'json':
        df = pd.read_csv(StringIO(data))
        return df.to_json(orient='records') # json is list of dictionaries (each represents a row)
    # JSON to CSV
    elif source_format == 'json' and target_format == 'csv':
        df = pd.read_json(StringIO(data))
        return df.to_csv(index=False)
    
    # if SQL then just return raw as we don't want to convert before storing (we want to convert the dataframe to sql after modifying)
    return data

# convert nested JSON to strings
def flatten_json_values(df):
    for column in df.columns:
        df[column] = df[column].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
    return df


"""
3) modify number of columns of data
    - reduce or add cols
    - for add you can put any info you want
"""
def modify_columns(data, target_format, columns_to_keep=None, columns_to_add=None):

    df = pd.read_csv(StringIO(data)) if target_format in ['csv', 'sql'] else pd.read_json(StringIO(data))

    if columns_to_keep:
        df = df[columns_to_keep]
    
    if columns_to_add:
        for col, value in columns_to_add.items():
            df[col] = value
    
    if target_format in ['csv', 'sql']: # only will be csv or json because for sql the conversion is done after this
        return df.to_csv(index=False)
    else:
        return df.to_json(orient='records')

"""
4) store new file
    - either to local files or sql database
"""
def store_data(data, target_format, output_file_path, database_name=None, table_name=None):
    if target_format == 'sql':
        try:
            # try reading the csv data into a df
            df = pd.read_csv(StringIO(data))
        except pd.errors.ParserError as e:
            raise ValueError(f"Error parsing CSV data: {str(e)}")
        
        # flatten any json-like values if needed
        df = flatten_json_values(df)
        
        # connect to sqlite and store data
        try:
            conn = sqlite3.connect(database_name)
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            conn.close()
            print(f"Data successfully stored in {table_name} table in {database_name}")
        except sqlite3.Error as e:
            raise ValueError(f"Error storing data in SQLite: {str(e)}")
    
    else:  # for csv or json
        with open(output_file_path, 'w') as file:
            file.write(data)


"""
5/6) generate summary of data file ingestion or summary
    - include: num records, num cols
"""
def generate_summary(data, format):
    df = pd.read_csv(StringIO(data)) if format == 'csv' else pd.read_json(StringIO(data))
    return f"Number of records: {len(df)}\n Number of columns: {len(df.columns)}"


# processor function
def process_data(source, output_file_path, columns_to_keep=None, columns_to_add=None):
    try:
        # fetch data
        print("Fetching data...")
        raw_data = fetch_data(source)

        # check format
        source_format = detect_file_format(raw_data)
        print(f"File format: {format}")

        # convert format
        target_format = input("To which format would you like to convert the data?\n'csv' for CSV | 'json' for JSON | 'sql' for SQL table:\n").lower()
        if target_format not in ['csv', 'json', 'sql']:
            raise ValueError("Invalid output format. Please choose csv, json, or sql.")
        
        if target_format == 'sql':
            database_name = input("Enter the database name (Ex. 'output.db'): ")
            table_name = input("Enter the table name: ")

        print("Converting data...")
        converted_data = convert_data_format(raw_data, source_format, target_format)

        # generate summary
        ingestion_summary = generate_summary(raw_data, source_format)
        print(f"Ingestion summary:\n {ingestion_summary}")

        # modify columns
        print("Modifying columns...")
        modified_data = modify_columns(converted_data, target_format, columns_to_keep, columns_to_add)

        # store data
        print("Storing data...")
        if target_format == 'sql':
            store_data(modified_data, target_format, output_file_path, database_name, table_name)
        else:
            store_data(modified_data, target_format, output_file_path + "." + target_format)

        # generate post processing summary
        if target_format == 'sql':
            # query from sql
            conn = sqlite3.connect(database_name)
            query = f"SELECT COUNT(*) as num_records, * FROM {table_name} LIMIT 1"
            result_df = pd.read_sql_query(query, conn)
            num_records = result_df['num_records'].values[0]
            num_columns = len(result_df.columns) - 1  # Excluding the "num_records" column
            conn.close()
            print(f"Post-processing summary:\n Number of records: {num_records}\n Number of columns: {num_columns}")
        else:
            # generate post processing summary for csv or json
            post_summary = generate_summary(modified_data, target_format)
            print(f"Post-processing summary:\n {post_summary}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

    return;


if __name__ == "__main__":
    # Example tests
    # process_data("cville_residential.csv", "c:/Users/Angad Brar/Desktop/DS2002/Proj_1/outputCSVtoJSON")
    # process_data("https://api.github.com/users/angadv12/repos", "c:/Users/Angad Brar/Desktop/DS2002/Proj_1/outputJSONtoCSV")
    process_data("cville_residential.csv", "c:/Users/Angad Brar/Desktop/DS2002/Proj_1/outputCSVtoSQL")
    process_data("https://api.github.com/users/angadv12/repos", "c:/Users/Angad Brar/Desktop/DS2002/Proj_1/outputJSONtoSQL")