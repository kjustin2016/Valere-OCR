import boto3
import os
import re
from dotenv import load_dotenv

# Enter the exact string of characters that you want to find at search_string
# Example, if looking for prescriptions enter 'PRESCRIPTION'

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

# Function to search for a substring in the object names and return the corresponding line number(s)
def find_object_line_by_substring(substring):
    matching_lines = []
    # Loop through all the object names and check if the substring is present
    for i, obj_name in enumerate(object_keys):
        if substring.lower() in obj_name.lower():  # Case-insensitive match
            matching_lines.append(i + 1)  # Line number is 1-based
    return matching_lines

# Example usage of the search function
search_string = 'PRESCRIPTION'  # Example: search for "PRESCRIPTION"
matching_lines = find_object_line_by_substring(search_string)

# Output the results
if matching_lines:
    print(f"Objects containing '{search_string}' found at line numbers: {', '.join(map(str, matching_lines))}")
else:
    print(f"No objects found containing '{search_string}'.")

# Save output to a fixed file called "outputName.txt"
output_file = 'outputName.txt'

# Save the matching line numbers to the specified file
with open(output_file, 'w') as f:
    if matching_lines:
        f.write(f"Objects containing '{search_string}' found at line numbers: {', '.join(map(str, matching_lines))}\n")
    else:
        f.write(f"No objects found containing '{search_string}'.\n")

print(f"\n✅ Results saved to 'outputName.txt' successfully!")
