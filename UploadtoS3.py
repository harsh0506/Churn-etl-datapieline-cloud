import boto3
import os
from datetime import datetime

access_key = os.getenv("access_key")
secret_access_key = os.getenv("secret_access_key")
region = os.getenv("region")

def upload_file_to_s3(file_path, bucket_name, s3_folder, access_key, secret_access_key, region ,naming_convention):
    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key, region_name=region)

    # Extract the file name and extension
    file_name = file_path.split("/")[-1]
    file_base, file_ext = os.path.splitext(file_name)

    # Define the S3 key (path) based on the naming convention
    if naming_convention == 'original':
        s3_key = f'{s3_folder}/{file_name}'
    elif naming_convention == 'archieve':
        current_date = datetime.now().strftime('%Y%m%d')
        s3_key = f'{s3_folder}/{file_base}_{current_date}{file_ext}'
    elif naming_convention == 'date_and_time':
        current_datetime = datetime.now().strftime('%Y%m%d%H%M%S')
        s3_key = f'{s3_folder}/{current_datetime}{file_ext}'
        print(s3_key)
    else:
        raise ValueError('Invalid naming convention. Supported values are: original, name_and_date, date_and_time')

    try:
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f'Successfully uploaded {file_path} to {bucket_name}/{s3_key}')
    except Exception as e:
        print(f'Error uploading {file_path} to {bucket_name}/{s3_key}: {str(e)}')

# Example usage:
#upload_file_to_s3('ml models\gbm.pkl', 'luffydatalake', 'model', access_key, secret_access_key, region)

#upload_file_to_s3(file_path='churn.csv', bucket_name='luffydatalake', s3_folder='archieve', access_key=access_key,secret_access_key=secret_access_key, region=region, naming_convention='archieve')

