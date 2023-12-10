import pymysql
import pandas as pd
import os
from UploadtoS3 import upload_file_to_s3
import csv
from dotenv import load_dotenv
import boto3
from datetime import datetime

load_dotenv()
access_key = os.getenv("access_key")
secret_access_key = os.getenv("secret_access_key")
region = os.getenv("region")


def Create_Analyics_reports():
    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key, region_name=region)

    csv_names = ['customer_profile_data','Distinct_Gender_Count','Distinct_Country_Count','Summary_Statistics',
                'Country_Credit_Score_Distribution','Churn_Effect_on_Financials_and_Gender','Age_Range_Churn_CreditCard_Insights',
                'Countrywise_Active_Member_Churn_Analysis','High_Balance_to_Salary_Customers','Low_Balance_to_Salary_Customers'
                ]

    # Establish a connection to the MySQL database
    conn = pymysql.connect(host=os.getenv("DB_HOST"), user=os.getenv("DB_USERNAME"), password=os.getenv("DB_PASSWORD"), ssl={'ssl_disabled': True})

    # Read and execute SQL statements from the .sql file
    with open('sql_to_table.sql', 'r') as sql_file:
        sql_statements = sql_file.read().split(';')

    cursor = conn.cursor()
    folder_path = 'analytics reports'

    for i, sql_statement in enumerate(sql_statements):
        if sql_statement.strip():
            try:
                cursor.execute(sql_statement)
                result = cursor.fetchall()

                if result:
                    # Retrieve data and store in a DataFrame
                    df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
                    

                    # Check if the folder exists, create it if not
                    if not os.path.exists(folder_path):
                        os.makedirs(folder_path)

                    # Specify the CSV file name
                    csv_file_name = f'{folder_path}/{csv_names[i]}.csv'

                    df.to_csv(csv_file_name, index=False)
            except Exception as e:
                print(f"Error processing SQL statement {i}: {str(e)}")
                continue

    # Close the connection
    conn.close()
    
    def upload_folder_to_s3(local_folder, s3_bucket, s3_folder):

        # Get a list of all files in the local folder
        for root, dirs, files in os.walk(local_folder):
            for file in files:
                local_file_path = os.path.join(root, file)
                # Define the S3 key (path) based on the original folder structure
                cur_date= datetime.now().strftime('%Y%m%d%H%M')
                s3_key = f'{s3_folder}/{cur_date}/{os.path.relpath(local_file_path, local_folder)}'
                
                try:
                    # Upload each file to S3
                    s3.upload_file(local_file_path, s3_bucket, s3_key)
                    print(f'Successfully uploaded {local_file_path} to {s3_bucket}/{s3_key}')
                except Exception as e:
                    print(f'Error uploading {local_file_path} to {s3_bucket}/{s3_key}: {str(e)}')
                    
    upload_folder_to_s3(folder_path,"luffydatalake","analyticsreport")
    