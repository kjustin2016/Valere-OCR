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
    object_keys = []
    paginator = s3.get_paginator('list_objects_v2')  # checks page by page
    operation_parameters = {'Bucket': bucket}
    page_iterator = paginator.paginate(**operation_parameters)

    for page in page_iterator:
        if 'Contents' in page and page['Contents']:
            object_keys.extend([item['Key'] for item in page['Contents']])
    
    return object_keys

def get_s3_bucket_object(bucket, index=0):
    paginator = s3.get_paginator('list_objects_v2')  # checks page by page
    operation_parameters = {'Bucket': bucket}
    page_iterator = paginator.paginate(**operation_parameters)

    object_keys = []
    for page in page_iterator:
        if 'Contents' in page and page['Contents']:
            object_keys.extend([item['Key'] for item in page['Contents']])
    
    if len(object_keys) > index:
        return object_keys[index]  # Return the object key at the specified index
    else:
        print(f"Less than {index + 1} objects found in the bucket.")
        return None

def detect_signature(bucket, object_key):
    from textractor.parsers import response_parser

    try:
        response = textract.analyze_document(
            Document={'S3Object': {'Bucket': bucket, 'Name': object_key}},
            FeatureTypes=['SIGNATURES']
        )
        tdoc = response_parser.parse(response)
        for signature in tdoc.signatures:
            print(f"Signature detected: {signature}")
            return True
        print("No signatures detected.")
        return False
    except Exception as e:
        print(e)
        return False

def structure_text(text):
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

    return sections

def determine_document_type(text):
    if "Prescription" in text:
        return "Prescription"
    elif "Agreement" in text:
        return "Signed Agreement"
    else:
        return "Unknown"

def extract_information(text):
    patient_info = {
        "Mother Name": None,
        "Mother Date of Birth": None,
        "Mother Phone Number": None,
        "Infant Name": None,
        "Infant Date of Birth": None
    }

    physician_info = {
        "Physician Name": None,
        "Physician NPI": None,
        "Date": None
    }

    lines = text.split('\n')
    for line in lines:
        print(f"Processing line: {line}")  # Debug print statement
        if "Mother Name" in line:
            patient_info["Mother Name"] = line.split(":")[1].strip()
        elif "Mother Date of Birth" in line:
            patient_info["Mother Date of Birth"] = line.split(":")[1].strip()
        elif "Mother Phone Number" in line:
            patient_info["Mother Phone Number"] = line.split(":")[1].strip()
        elif "Infant Name" in line:
            patient_info["Infant Name"] = line.split(":")[1].strip()
        elif "Infant Date of Birth" in line:
            patient_info["Infant Date of Birth"] = line.split(":")[1].strip()
        elif "Physicians Name" in line:
            # Split by "Physicians Name:" and take the first part before "Physician NPI"
            parts = line.split("Physician NPI:")
            physician_info["Physician Name"] = parts[0].split(":")[1].strip()
            if len(parts) > 1:
                physician_info["Physician NPI"] = parts[1].strip()
                print(f"Extracted Physician NPI: {physician_info['Physician NPI']}")  # Additional debug statement
            else:
                print("Physician NPI not found in line:", line)  # Additional debug statement
        elif "Physician NPI" in line and "Physicians Name" not in line:
            # Handle case where Physician NPI is on a separate line
            physician_info["Physician NPI"] = line.split(":")[1].strip()
            print(f"Extracted Physician NPI from separate line: {physician_info['Physician NPI']}")  # Additional debug statement
        elif "Date" in line:
            # Split by "Date:" and take the last part
            date_parts = line.split("Date:")
            if len(date_parts) > 1:
                physician_info["Date"] = date_parts[1].strip()
                print(f"Extracted Date: {physician_info['Date']}")  # Additional debug statement
            else:
                print("Date not found in line:", line)  # Additional debug statement

    return {
        "Patient Info": patient_info,
        "Physician Info": physician_info
    }

def extract_information_signed_agreement(text):
    agreement_info = {
        "Customer/Patient Name": None,
        "Signature Present": "No"
    }

    lines = text.split('\n')
    for line in lines:
        print(f"Processing line: {line}")  # Debug print statement
        if "Customer/Patient Name" in line:
            agreement_info["Customer/Patient Name"] = line.split(":")[1].strip()

    return agreement_info

if __name__ == "__main__":
    bucket_name = "capstone-intelligent-document-processing"
    print(f"Bucket name: {bucket_name}")
    
    object_key = get_s3_bucket_object(bucket_name, index=0)  # Change to index 0 for the first document
    print(f"Object key: {object_key}")
    
    if object_key:
        text_data = textract_extract_text(bucket_name, object_key)
        structured_text = structure_text(text_data[1])
        signature_present = detect_signature(bucket_name, object_key)
        
        # Determine document type and extract specific information
        document_type = determine_document_type(text_data[1])
        if document_type == "Prescription":
            extracted_info = extract_information(text_data[1])
        elif document_type == "Signed Agreement":
            extracted_info = extract_information_signed_agreement(text_data[1])
            extracted_info["Signature Present"] = "Yes" if signature_present else "No"
        else:
            extracted_info = {"Error": "Unknown document type"}
        
        # Format as JSON
        json_output = json.dumps(extracted_info, indent=4)
        print(f"Extracted Information in JSON format:\n{json_output}")
        
        # Optionally, save the JSON output to a file
        with open("extracted_info.json", "w") as json_file:
            json_file.write(json_output)
    else:
        print("No documents found in the S3 bucket.")