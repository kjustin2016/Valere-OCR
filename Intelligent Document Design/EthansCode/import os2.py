import boto3
import os
import hashlib
from dotenv import load_dotenv
from PIL import Image
import webbrowser

# At the bottom, comment out either the index [2] or the entity_tag depending on which one you use

load_dotenv()

# AWS Credentials (optional if configured via AWS CLI or environment variables)
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")
bucket_name = 'capstone-intelligent-document-processing'

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)

# Get a list of all object keys and their ETags in the S3 bucket
def list_s3_objects(bucket_name):
    s3_keys = []
    s3_etags = {}
    response = s3_client.list_objects_v2(Bucket=bucket_name)

    if 'Contents' in response:
        for obj in response['Contents']:
            s3_keys.append(obj['Key'])
            s3_etags[obj['Key']] = obj['ETag'].strip('"')  # Store ETag without quotes
    else:
        print('No objects found in the bucket.')
    return s3_keys, s3_etags

# Fetch all document keys and their corresponding ETags
s3_keys, s3_etags = list_s3_objects(bucket_name)

# Compute file hash (MD5) for integrity check
def compute_file_hash(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

# Validate if the file is a proper image using Pillow
def validate_image(file_path):
    try:
        with Image.open(file_path) as img:
            img.verify()  # Check if it's a valid image
            print(f"✅ The file {file_path} is a valid image.")
            return True
    except Exception as e:
        print(f"❌ Error validating image: {e}")
        return False

# Open the file in the default application using os.startfile (Windows only)

# Open the file in the default browser
def open_file(file_path):
    try:
        # Convert the file path to a valid file URL format
        file_url = 'file:///' + os.path.abspath(file_path).replace("\\", "/")  # Correct path format for browsers
        print(f"Opening file in browser: {file_url}")
        webbrowser.open(file_url, new=2)  # 'new=2' opens the file in a new tab
    except Exception as e:
        print(f"Error opening file: {e}")

# Download and open the file based on the provided key index or entity tag
def open_file_from_s3(index=None, entity_tag=None):
    if index is not None:
        # Validate index
        if index < 0 or index >= len(s3_keys):
            print(f'Error: Index {index} is out of range. Valid range is 0 to {len(s3_keys) - 1}.')
            return
        # Get the object key based on the given index
        object_key = s3_keys[index]
    elif entity_tag is not None:
        # Find the key corresponding to the given entity tag
        object_key = None
        for key, etag in s3_etags.items():
            if etag == entity_tag:
                object_key = key
                break
        
        if object_key is None:
            print(f'Error: No object found with the specified entity tag: {entity_tag}')
            return
    else:
        print('Error: You must provide either an index or an entity tag.')
        return
    
    # Set the local path to save the file in the "Downloads" folder
    downloads_path = os.path.join(os.path.expanduser('~'), 'Downloads')
    local_file_name = os.path.join(downloads_path, f'temp_{object_key.split("/")[-1]}')

    # Remove any unwanted suffix from file name (e.g., .null.jpg)
    local_file_name = local_file_name.replace('.null.jpg', '')

    # Check if the file already exists and is a valid image
    if os.path.exists(local_file_name):
        print(f"✅ File already exists: {local_file_name}")
        if validate_image(local_file_name):  # Validate if the file is a proper image
            open_file(local_file_name)
        return

    try:
        # Download the file in binary mode if it doesn't exist
        with open(local_file_name, 'wb') as file:
            s3_client.download_fileobj(bucket_name, object_key, file)
        
        # Verify the downloaded file size and integrity
        file_size = os.path.getsize(local_file_name)
        print(f'File downloaded with size: {file_size} bytes.')

        if file_size == 0:
            print(f"❌ The downloaded file appears empty. Check the download process.")
            return
        
        # Validate the image using Pillow
        if validate_image(local_file_name):
            # Compute and compare file hash (MD5) for integrity
            local_hash = compute_file_hash(local_file_name)
            print(f"Local file hash: {local_hash}")
        
        # Open the file using the default associated application
        open_file(local_file_name)

    except Exception as e:
        print(f'❌ Error downloading or processing file: {e}')

# Example: Open a document using its index (0-based index, so index 33)
open_file_from_s3(index=51)

# Example: Open a document using its entity tag
#open_file_from_s3(entity_tag="b07034666f0fbee7461b1202a52e2cc5")