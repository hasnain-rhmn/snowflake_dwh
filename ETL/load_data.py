import os
import boto3
from botocore.exceptions import NoCredentialsError
import shutil
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def upload_files_to_s3_and_archive(local_dir, bucket_name, processed_dir):
    # Initialize S3 client with environment variables
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    )

    # Create S3 bucket if it doesn't exist
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except:
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': os.getenv('AWS_DEFAULT_REGION', 'us-east-1')}
        )
        print(f"Created bucket: {bucket_name}")

    # Handle the dataset directories: listings, calendar, reviews
    datasets = ["listings", "calendar", "reviews"]
    
    for dataset in datasets:
        dataset_dir = os.path.join(local_dir, dataset)

        if os.path.isdir(dataset_dir):
            for file in os.listdir(dataset_dir):
                file_path = os.path.join(dataset_dir, file)
                s3_key = f"{dataset}/{file}"

                try:
                    print(f"Uploading {file_path} to s3://{bucket_name}/{s3_key}...")
                    s3.upload_file(file_path, bucket_name, s3_key)
                    
                    # Move file to processed directory
                    processed_dataset_dir = os.path.join(processed_dir, dataset)
                    os.makedirs(processed_dataset_dir, exist_ok=True)
                    shutil.move(file_path, os.path.join(processed_dataset_dir, file))
                    print(f"Moved {file_path} to {processed_dataset_dir}\n")
                
                except FileNotFoundError:
                    print(f"File not found: {file_path}")
                except NoCredentialsError:
                    print("AWS credentials not found. Please configure them.")


local_dir = "airbnb_data"
processed_dir = "airbnb_data_processed"
bucket_name = "my-airbnb-data-bucket"

upload_files_to_s3_and_archive(local_dir, bucket_name, processed_dir)
