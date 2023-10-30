import os
import pymysql
import csv
from datetime import datetime
from DownloadCsvFile import download_csv_from_s3

def batch_insert_data(rows):
    try:
        # Database connection parameters
        db_host = os.getenv("DB_HOST")
        db_username = os.getenv("DB_USERNAME")
        db_password = os.getenv("DB_PASSWORD")
        db_name = os.getenv("DB_NAME")

        # Establish a connection to the MySQL database
        conn = pymysql.connect(
            host=db_host,
            user=db_username,
            password=db_password,
            database=db_name,
            ssl={'ssl_disabled': True}  # Modify this as needed
        )

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS customer_data (
            customer_id INT,
            credit_score INT,
            country VARCHAR(255),
            gender VARCHAR(255),
            age INT,
            tenure INT,
            balance DECIMAL(10, 2),
            products_number INT,
            credit_card INT,
            active_member INT,
            estimated_salary DECIMAL(10, 2),
            churn INT,
            date_of_insertion DATETIME
        );
        """

        cursor = conn.cursor()

        cursor.execute(create_table_sql)

        insert_query = """
        INSERT INTO customer_data (customer_id, credit_score, country, gender, age, tenure, balance, products_number, credit_card, active_member, estimated_salary, churn, date_of_insertion)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        cursor.executemany(insert_query, rows)

        conn.commit()

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        if 'conn' in locals() and conn is not None:
            cursor.close()
            conn.close()

if not os.path.exists('churn.csv'):
    access_key = os.getenv("access_key")
    secret_access_key = os.getenv("secret_access_key")
    region = os.getenv("region")
    s3_bucket = 'luffydatalake'
    s3_key = 'temp/churn.csv'

    download_csv_from_s3(access_key, secret_access_key, region, s3_bucket, s3_key, 'churn.csv')

csv_file = 'churn.csv'

# Read the CSV file into a list of rows
rows = []
with open(csv_file, 'r') as file:
    csv_reader = csv.reader(file)
    next(csv_reader)  # Skip the header row
    for row in csv_reader:
        today_date = datetime.now().strftime('%Y-%m-%d')
        row.append(today_date)
        rows.append(row)

# Batch insert the rows into the database
batch_insert_data(rows)
