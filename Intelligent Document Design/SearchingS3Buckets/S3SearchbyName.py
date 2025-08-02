import boto3
import os
import re
import textwrap
from dotenv import load_dotenv

search_string = 'FACESHEET'  # Example: search for "PRESCRIPTION"


load_dotenv()

# AWS Credentials (optional if configured via AWS CLI or environment variables)
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")
bucket_name = 'capstone-intelligent-document-processing'

# Initialize S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=aws_region  # Ensure you include the region
)

def get_all_object_names(bucket_name):
    paginator = s3.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket_name}
    page_iterator = paginator.paginate(**operation_parameters)

    all_object_keys = []  # Array to store object names
    for page in page_iterator:
        if 'Contents' in page:
            object_keys = [item['Key'] for item in page['Contents']]
            all_object_keys.extend(object_keys)
    
    return all_object_keys

def filter_objects_by_string(bucket_name, search_string):
    object_names_array = get_all_object_names(bucket_name)
    
    # Filter indices of object names that contain the search string
    matching_objects = [index for index, name in enumerate(object_names_array) if search_string.lower() in name.lower()]
    
    print(matching_objects)
    output_file = 'outputNames.txt'
    with open(output_file, 'w') as f:
        if matching_objects:
            # Prepare the output string
            output_text = f"Objects containing '{search_string}' found at indices: {', '.join(map(str, matching_objects))}\n"
        else:
            output_text = f"No objects found containing '{search_string}'.\n"
        
        # Wrap the text to 50 characters per line
        wrapped_text = textwrap.fill(output_text, width=50)
        
        # Write the wrapped text to the file
        f.write(wrapped_text)
    
    return matching_objects

def filter_objects_by_name(bucket_name, search_string):
    object_names_array = get_all_object_names(bucket_name)
    
    # Filter object names that contain the search string
    matching_objects = [name for name in object_names_array if search_string.lower() in name.lower()]
    
    print(matching_objects)
    output_file = 'outputNamesByName.txt'
    with open(output_file, 'w') as f:
        if matching_objects:
            # Write each object name on a new line with the index from object_names_array at the start
            for name in matching_objects:
                index_in_array = object_names_array.index(name)
                f.write(f"{index_in_array}. {name}\n")
        else:
            f.write(f"No objects found containing '{search_string}'.\n")
    
    return matching_objects

# Call the function
filter_objects_by_name('capstone-intelligent-document-processing', search_string)

# filter_objects_by_string('capstone-intelligent-document-processing', search_string)
