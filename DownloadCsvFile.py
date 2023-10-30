import os
import boto3

def download_csv_from_s3(access_key, secret_access_key, region, bucket, s3_key, local_file_path):
    try:
        # Check if the local file already exists
        if os.path.exists(local_file_path):
            print(f"Local file '{local_file_path}' already exists. Skipping download.")
            return

        # Initialize the S3 client
        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key, region_name=region)

        # Check if the S3 object exists
        response = s3.head_object(Bucket=bucket, Key=s3_key)

        if 'ContentLength' not in response:
            raise Exception(f"S3 object '{s3_key}' in bucket '{bucket}' does not have a ContentLength.")
        
        # Download the file from S3
        with open(local_file_path, 'wb') as f:
            s3.download_fileobj(bucket, s3_key, f)
            print(f"Downloaded S3 object '{s3_key}' to '{local_file_path}'")
    except Exception as e:
        print(f"Error during S3 download: {str(e)}")