import os
import boto3
import json
import time
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
    
    return object_keys[19]  # Return the first object key (index 0)

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

def extract_information(response):
    patient_info = {}
    doctor_info = {}

    key_map = {}
    value_map = {}
    block_map = {}

    blocks = response.get('Blocks', [])
    if not blocks:
        raise ValueError("No blocks found in Textract response")

    for block in blocks:
        block_id = block['Id']
        block_map[block_id] = block
        if block['BlockType'] == "KEY_VALUE_SET":
            if 'KEY' in block['EntityTypes']:
                key_map[block_id] = block
            else:
                value_map[block_id] = block

    def get_kv_relationship(key_map, value_map, block_map):
        kvs = {}
        for block_id, key_block in key_map.items():
            value_block = None
            for relationship in key_block.get('Relationships', []):
                if relationship['Type'] == 'VALUE':
                    for value_id in relationship['Ids']:
                        value_block = value_map.get(value_id)
                        break
            key = get_text(key_block, block_map)
            value = get_text(value_block, block_map)
            kvs[key] = value
        return kvs

    def get_text(result, blocks_map):
        text = ''
        if result and 'Relationships' in result:
            for relationship in result['Relationships']:
                if relationship['Type'] == 'CHILD':
                    for child_id in relationship['Ids']:
                        word = blocks_map[child_id]
                        if word['BlockType'] == 'WORD':
                            text += word['Text'] + ' '
                        if word['BlockType'] == 'SELECTION_ELEMENT':
                            if word['SelectionStatus'] == 'SELECTED':
                                text += 'X '
        return text.strip()

    kvs = get_kv_relationship(key_map, value_map, block_map)

    for key, value in kvs.items():
        if "Mother" in key or "Infant" in key:
            patient_info[key] = value
        elif "Physician" in key or "Doctor" in key:
            doctor_info[key] = value

    return {
        "patient": patient_info,
        "doctor": doctor_info
    }

if __name__ == "__main__":
    bucket_name = "capstone-intelligent-document-processing"
    
    object_key = get_s3_bucket_object(bucket_name)
    
    if object_key:
        text_data = textract_extract_text(bucket_name, object_key)
        if text_data:
            # Extract and structure the text
            structured_text, full_text = structure_text(text_data[0])  # Pass the response directly
            
            # Extract the first line of text (from the full_text variable) for the label
            first_line = full_text.split('\n')[0].strip() if full_text else "No Label Found"
            
            # Extract specific information and format as JSON
            extracted_info = extract_information(text_data[0])
            
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
        print("No documents found in the S3 bucket.")
