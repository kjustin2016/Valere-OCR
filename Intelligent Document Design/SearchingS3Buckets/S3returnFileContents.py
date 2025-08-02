import boto3
import os
import hashlib
from dotenv import load_dotenv
from PIL import Image
import webbrowser
import re

# Returns a numbered list of important words from every object in S3

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
    aws_secret_access_key=aws_secret_key,
    region_name=aws_region  # Ensure you include the region
)

# Array to store object keys
object_keys = []

# Array to store formatted document results
document_lines = []

# Function to extract real words/phrases from object names
def extract_words(obj_name):
    # Match real words/phrases with optional file extensions
    word_pattern = re.findall(r'[A-Za-z]+(?:_[A-Za-z]+)*(?:pdf|jpg|jpeg|png)?', obj_name)
    
    # Filter words: remove short strings (length < 3 after removing underscores)
    real_words = [word for word in word_pattern if len(word.replace('_', '')) > 2]
    
    # Remove duplicates and sort words for consistency
    return sorted(set(real_words))

# Pagination setup
continuation_token = None

# Loop to handle pagination and get all objects
while True:
    # List objects with continuation token if present
    list_params = {'Bucket': bucket_name}
    if continuation_token:
        list_params['ContinuationToken'] = continuation_token
    
    response = s3_client.list_objects_v2(**list_params)
    
    # Add the object keys to the list
    if 'Contents' in response:
        for obj in response['Contents']:
            object_name = obj['Key']
            object_keys.append(object_name)
    
    # Check if there are more objects to retrieve
    if response.get('IsTruncated'):  # If there are more objects
        continuation_token = response.get('NextContinuationToken')
    else:
        break

# Define the range of indices to process
start_index = 0
end_index = len(object_keys)  # Automatically uses the total number of objects retrieved

# Process objects within the specified range
for i in range(start_index, end_index):
    object_name = object_keys[i]
    
    # Extract words/phrases from the object name
    words = extract_words(object_name)
    
    # Only add if there are meaningful words found
    if words:
        document_line = f"{i + 1}. {', '.join(words)}"
        document_lines.append(document_line)

# Save output to output.txt and print results
with open('output.txt', 'w') as f:
    for line in document_lines:
        f.write(line + '\n')
        print(line)

print(f"\n✅ Output saved to 'output.txt' successfully!")
