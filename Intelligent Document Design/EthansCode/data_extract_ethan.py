import os
import boto3
import json
from dotenv import load_dotenv
from textractcaller.t_call import call_textract
from textractprettyprinter.t_pretty_print import (Textract_Pretty_Print, get_string)

load_dotenv()

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")

print(f"AWS Access Key ID: {aws_access_key_id}")
print(f"AWS Secret Access Key: {aws_secret_access_key}")
print(f"AWS Region: {aws_region}")

s3 = boto3.client('s3',
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key)

textract = boto3.client('textract',
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        region_name=aws_region)

def textract_extract_text(bucket, object_key): 
    try:
        print(f"Extracting text from bucket: {bucket}, object key: {object_key}")
        row = []
        response = call_textract(input_document=f's3://{bucket}/{object_key}', boto3_textract_client=textract) 
        lines = get_string(textract_json=response, output_type=[Textract_Pretty_Print.LINES])
        row.append(create_document_label(object_key, lines))
        row.append(lines)
        return row
    except Exception as e:
        print(e)

def create_document_label(object_key, document_lines):
    label = ""
    # TODO: create label
    return label

def get_s3_bucket_objects(bucket):
    print(f"Getting objects from bucket: {bucket}")
    object_keys = []
    paginator = s3.get_paginator('list_objects_v2')  # checks page by page
    operation_parameters = {'Bucket': bucket}
    page_iterator = paginator.paginate(**operation_parameters)

    for page in page_iterator:
        if 'Contents' in page and page['Contents']:
            object_keys.extend([item['Key'] for item in page['Contents']])
    
    return object_keys

def get_s3_bucket_object(bucket):
    print(f"Getting objects from bucket: {bucket}")
    paginator = s3.get_paginator('list_objects_v2')  # checks page by page
    operation_parameters = {'Bucket': bucket}
    page_iterator = paginator.paginate(**operation_parameters)

    object_keys = []
    for page in page_iterator:
        if 'Contents' in page and page['Contents']:
            object_keys.extend([item['Key'] for item in page['Contents']])
    
    if len(object_keys) >= 3:
        return object_keys[2]  # Return the third object key (index 2)
    else:
        print("Less than 3 objects found in the bucket.")
        return None

def detect_signature(bucket, object_key):
    from textractor.parsers import response_parser

    try:
        print(f"Detecting signatures from bucket: {bucket}, object key: {object_key}")
        response = textract.analyze_document(
            Document={'S3Object': {'Bucket': bucket, 'Name': object_key}},
            FeatureTypes=['SIGNATURES']
        )
        print(f"Textract response: {response}")
        tdoc = response_parser.parse(response)
        for signature in tdoc.signatures:
            print(f"Signature detected: {signature}")
            return signature.bbox, f"Confidence: {signature.confidence}\n"
    except Exception as e:
        print(e)
        return None

def structure_text(text):
    sections = {}
    current_section = None

    for line in text.split('\n'):
        print(f"Processing line: {line}")  # Debug print statement
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

    return sections

def print_structured_text(structured_text):
    for section, content in structured_text.items():
        print(f"\n{section}\n{'-' * len(section)}")
        print(content)
        print("\n")  # Add a line break before the next section

def extract_information(text):
    info = {
        "Mother Name": None,
        "Mother Date of Birth": None,
        "Mother Phone Number": None,
        "Infant Name": None,
        "Infant Date of Birth": None
    }

    lines = text.split('\n')
    for line in lines:
        if "Mother Name" in line:
            info["Mother Name"] = line.split(":")[1].strip()
        elif "Mother Date of Birth" in line:
            info["Mother Date of Birth"] = line.split(":")[1].strip()
        elif "Mother Phone Number" in line:
            info["Mother Phone Number"] = line.split(":")[1].strip()
        elif "Infant Name" in line:
            info["Infant Name"] = line.split(":")[1].strip()
        elif "Infant Date of Birth" in line:
            info["Infant Date of Birth"] = line.split(":")[1].strip()

    return info

if __name__ == "__main__":
    bucket_name = "capstone-intelligent-document-processing"
    print(f"Bucket name: {bucket_name}")
    
    object_key = get_s3_bucket_object(bucket_name)
    print(f"Object key: {object_key}")
    
    if object_key:
        text_data = textract_extract_text(bucket_name, object_key)
        print(f"Extracted text data: {text_data}")
        structured_text = structure_text(text_data[1])
        print(f"Structured text: {structured_text}")
        print_structured_text(structured_text)
        signatures = detect_signature(bucket_name, object_key)
        if signatures:
            print("Signatures extracted successfully.")
        else:
            print("No signatures found.")
        
        # Extract specific information and format as JSON
        extracted_info = extract_information(text_data[1])
        json_output = json.dumps(extracted_info, indent=4)
        print(f"Extracted Information in JSON format:\n{json_output}")
        
        # Optionally, save the JSON output to a file
        with open("extracted_info.json", "w") as json_file:
            json_file.write(json_output)
    else:
        print("No documents found in the S3 bucket.")