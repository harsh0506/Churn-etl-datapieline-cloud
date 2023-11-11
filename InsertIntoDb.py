import pandas as pd
from mysql.connector import connect
from dotenv import load_dotenv
import os
from datetime import datetime
from DownloadCsvFile import download_csv_from_s3

load_dotenv()

db_host = os.getenv("DB_HOST")
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")


def PopulateData():
    if not os.path.exists("churn.csv"):
        access_key = os.getenv("access_key")
        secret_access_key = os.getenv("secret_access_key")
        region = os.getenv("region")
        s3_bucket = "luffydatalake"
        s3_key = "temp/churn.csv"

        download_csv_from_s3(
            access_key, secret_access_key, region, s3_bucket, s3_key, "churn.csv"
        )

    # Load CSV into a DataFrame
    df = pd.read_csv("churn.csv")

    # Add current date as a new column
    df["date_of_insertion"] = datetime.now().strftime("%Y-%m-%d")

    # Establish MySQL connection
    connection = connect(
        user=db_username, password=db_password, host=db_host, database=db_name
    )
    cursor = connection.cursor()

    # Define the table name
    table_name = "customer_data2"

    # Batch insert with chunk size
    chunk_size = 10000
    for i in range(0, len(df), chunk_size):
        chunk_df = df.iloc[i : i + chunk_size]

        # Convert the DataFrame to a list of tuples
        values = [tuple(row) for row in chunk_df.to_numpy()]

        # Define the INSERT query
        insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))})"

        # Execute the batch insert
        cursor.executemany(insert_query, values)

    # Commit and close
    connection.commit()
    cursor.close()
    connection.close()

PopulateData()