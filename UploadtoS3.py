import boto3

def upload_file_to_s3(file_path, bucket_name, s3_folder, access_key, secret_access_key, region):
    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key, region_name=region)

    # Define the S3 key (path) where you want to upload the file
    s3_key = f'{s3_folder}/{file_path.split("/")[-1]}'  # Appending the filename to the folder

    try:
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f'Successfully uploaded {file_path} to {bucket_name}/{s3_key}')
    except Exception as e:
        print(f'Error uploading {file_path} to {bucket_name}/{s3_key}: {str(e)}')

# Example usage:
# upload_file_to_s3('local_file.txt', 'your-s3-bucket', 'your-s3-folder', 'your-access-key', 'your-secret-key', 'your-region')
