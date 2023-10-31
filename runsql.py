import pymysql
import pandas as pd
import os
import csv
from dotenv import load_dotenv
load_dotenv()

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

for i, sql_statement in enumerate(sql_statements):
    if sql_statement.strip():
        try:
            cursor.execute(sql_statement)
            result = cursor.fetchall()

            if result:
                # Retrieve data and store in a DataFrame
                df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])

                # Store the data in a CSV file
                csv_file_name = f'analytics reports/{csv_names[i]}.csv'
                df.to_csv(csv_file_name, index=False)
        except Exception as e:
            print(f"Error processing SQL statement {i}: {str(e)}")
            continue

# Close the connection
conn.close()
