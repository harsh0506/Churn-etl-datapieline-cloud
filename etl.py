"""
Load raw csv from s3

Load cleaned Data from csv to rds
Transform csv 

-- (different workslow step) Generate Analytics reports -- 
-- (different workslow step) Load Analytics report to s3 -- 
Migrate raw csv to s3 (data archieve folder)
Load cleaned csv to s3 (in temp and data archieve folder)
Delete raw and clean csv form system

"""

# Import required modules and functions
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import MinMaxScaler
import os
from DownloadCsvFile import download_csv_from_s3
from UploadtoS3 import upload_file_to_s3
from InsertIntoDb import PopulateData
from runsql import Create_Analyics_reports

# Declare the variables
access_key = os.getenv("access_key")
secret_access_key = os.getenv("secret_access_key")
region = os.getenv("region")
s3_bucket = "luffydatalake"
s3_key = "temp/churn.csv"

# check if the raw csv file exists in the system
if not os.path.exists("churn.csv"):
    # Download the csv using download function, provide the region ,s3 bucket name and credentials
    download_csv_from_s3(
        access_key, secret_access_key, region, s3_bucket, s3_key, "churn.csv"
    )

# Load Data from csv into rds database table
PopulateData()

# Generate Analytical reports using sql
Create_Analyics_reports()

# Load raw CSV into s3-bucjet archieve folders
upload_file_to_s3(
    file_path="churn.csv",
    bucket_name=s3_bucket,
    s3_folder="archieve",
    access_key=access_key,
    secret_access_key=secret_access_key,
    region=region,
    naming_convention="archieve",
)

# Transform the CSV using pandas
# Initiate Label encoder and Min-max scaler
label_encoder = LabelEncoder()
scaler = MinMaxScaler()

df = pd.read_csv("churn.csv")

df["balance_to_salary_ratio"] = np.where(
    df["balance"] == 0, 0, df["balance"] / df["estimated_salary"]
)
df["Credit_Utilization_Ratio"] = np.where(
    df["balance"] == 0, 0, df["balance"] / df["credit_score"]
)

score_ranges = [0, 600, 650, 700, 750, 850]
score_labels = ["Poor", "Fair", "Good", "Very Good", "Excellent"]

df["credit_score_range"] = pd.cut(
    df["credit_score"], bins=score_ranges, labels=score_labels
)

df["country"] = label_encoder.fit_transform(df["country"])
df["gender"] = label_encoder.fit_transform(df["gender"])
df["credit_score_range"] = label_encoder.fit_transform(df["credit_score_range"])

numerical_columns = df.select_dtypes(include=["number"]).columns
df[numerical_columns] = scaler.fit_transform(df[numerical_columns])

df.to_csv("cleaned_churn.csv", index=False)

# Load cleaned csv to s3 (in temp and data archieve folder)
upload_file_to_s3(
    file_path="cleaned_churn.csv",
    bucket_name="luffydatalake",
    s3_folder="temp",
    access_key=access_key,
    secret_access_key=secret_access_key,
    region=region,
    naming_convention="original",
)
upload_file_to_s3(
    file_path="cleaned_churn.csv",
    bucket_name="luffydatalake",
    s3_folder="archieve",
    access_key=access_key,
    secret_access_key=secret_access_key,
    region=region,
    naming_convention="archieve",
)

# Delete the files from loacl system
def DelCsvFile(name):
    # Check if the file exists before attempting to delete
    if os.path.exists(name):
        # Delete the file
        os.remove(name)
        print(f'{name} deleted successfully')
    else:
        print(f'{name} does not exist')
        
DelCsvFile("churn.csv")
DelCsvFile("cleaned_churn.csv")
DelCsvFile("analytics report")