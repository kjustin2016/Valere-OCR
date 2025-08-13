import os
import boto3
import json
import time
import re
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize AWS clients
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")

s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
textract = boto3.client('textract', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=aws_region)

def textract_extract_text(bucket, object_key):
    try:
        row = []
        # Ensure the correct S3 URL format is passed to Textract
        s3_url = f"s3://{bucket}/{object_key}"

        # Check file extension (PNG, JPEG, or PDF)
        file_extension = object_key.split('.')[-1].lower()

        if file_extension in ['png', 'jpg', 'jpeg']:  # Handle image files
            # Call AWS Textract directly using boto3 for image files
            response = textract.analyze_document(
                Document={'S3Object': {'Bucket': bucket, 'Name': object_key}},
                FeatureTypes=["FORMS"]
            )
        elif file_extension == 'pdf':  # Handle PDF files
            # Start document analysis for PDFs (asynchronously)
            response = textract.start_document_analysis(
                DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': object_key}},
                FeatureTypes=["FORMS"]
            )
            # Wait for the analysis to complete
            job_id = response['JobId']
            print(f"Started asynchronous job for PDF with JobId: {job_id}")

            # Wait for the job to finish (or implement a retry mechanism)
            while True:
                result = textract.get_document_analysis(JobId=job_id)
                status = result['JobStatus']
                if status in ['SUCCEEDED', 'FAILED']:
                    break
                print("Waiting for Textract to complete the analysis...")
                time.sleep(5)  # Wait for 5 seconds before checking again

            if status == 'SUCCEEDED':
                response = result  # Use the analysis result
            else:
                raise ValueError("Textract document analysis failed.")

        else:
            raise ValueError(f"Unsupported file format: {file_extension}")

        if isinstance(response, str):
            response = json.loads(response)  # Convert string to JSON dictionary if necessary

        if response is None:
            raise ValueError("Textract response is None")

        row.append(response)  # Store the response for later use
        return row

    except Exception as e:
        print(f"Error in textract_extract_text: {e}")
        return None

def get_s3_bucket_object(bucket):
    paginator = s3.get_paginator('list_objects_v2')  # checks page by page
    operation_parameters = {'Bucket': bucket}
    page_iterator = paginator.paginate(**operation_parameters)

    object_keys = []
    for page in page_iterator:
        if 'Contents' in page and page['Contents']:
            object_keys.extend([item['Key'] for item in page['Contents']])
    
    if object_keys:
        return object_keys[0]  # Return the first object key (index 0)
    return None

def get_s3_bucket_object_by_tag(bucket, target_tag_value):
    """
    Find an S3 object by its tag value
    
    Parameters:
    bucket (str): The name of the S3 bucket
    target_tag_value (str): The value of the tag to search for
    
    Returns:
    str: The key of the object with the matching tag, or None if not found
    """
    try:
        # List all objects in the bucket
        paginator = s3.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': bucket}
        page_iterator = paginator.paginate(**operation_parameters)
        
        # Iterate through all objects
        for page in page_iterator:
            if 'Contents' in page and page['Contents']:
                for item in page['Contents']:
                    object_key = item['Key']
                    
                    # Get the tags for this object
                    try:
                        response = s3.get_object_tagging(
                            Bucket=bucket,
                            Key=object_key
                        )
                        
                        # Check if the target tag exists
                        tags = response.get('TagSet', [])
                        for tag in tags:
                            # Check if any tag's value matches our target
                            if tag.get('Value') == target_tag_value:
                                return object_key
                    except Exception as e:
                        continue
        
        # If we get here, no matching tag was found
        return None
    
    except Exception as e:
        print(f"Error in get_s3_bucket_object_by_tag: {e}")
        return None

def structure_text(response):
    # Extract the text from the Textract response dictionary
    text = ""
    blocks = response.get('Blocks', [])
    
    # Iterate over the blocks to extract text (typically from 'WORD' or 'LINE' blocks)
    for block in blocks:
        if block['BlockType'] == 'WORD' or block['BlockType'] == 'LINE':
            text += block.get('Text', '') + '\n'
    
    # Split the text into sections based on the '\n' character
    sections = {}
    current_section = None

    for line in text.split('\n'):
        if line.strip() == "":
            current_section = None
        elif line.isupper():
            current_section = line
            sections[current_section] = []
        elif current_section:
            sections[current_section].append(line)
        else:
            current_section = "PARAGRAPH"
            if current_section not in sections:
                sections[current_section] = []
            sections[current_section].append(line)

    for section, content in sections.items():
        sections[section] = ' '.join(content).strip()

    return sections, text  # Return both sections and the full text for label extraction


if __name__ == "__main__":
    bucket_name = "capstone-intelligent-document-processing"
    
    # Specify the entity tag you're looking for
    entity_tag_to_find = "b07034666f0fbee7461b1202a52e2cc5"  # Replace this with the actual tag you're looking for
    
    # Retrieve the document key by its entity tag
    object_key = get_s3_bucket_object_by_tag(bucket_name, entity_tag_to_find)
    
    if object_key:
        print(f"Found document with entity tag '{entity_tag_to_find}' - Object Key: {object_key}")
        text_data = textract_extract_text(bucket_name, object_key)
        if text_data:
            # Extract and structure the text
            structured_text, full_text = structure_text(text_data[0])  # Pass the response directly
            
            # Extract the first line of text (from the full_text variable) for the label
            first_line = full_text.split('\n')[0].strip() if full_text else "No Label Found"
            
            # Extract specific information and format as JSON
            extracted_info = {"message": "Information extraction not implemented"}
            
            # Add the label to the extracted information
            extracted_info_with_label = {
                "document label": first_line,  # Use the first line as the document label
                "data": extracted_info
            }
            
            # Convert the extracted information to JSON format
            json_output = json.dumps(extracted_info_with_label, indent=4)
            print(f"Extracted Information in JSON format:\n{json_output}")
            
            # Optionally, save the JSON output to a file
            with open("extracted_info.json", "w") as json_file:
                json_file.write(json_output)
        else:
            print("Failed to extract text data from Textract response.")
    else:
        print(f"No document found with entity tag '{entity_tag_to_find}' in the S3 bucket.")