import os
import re
import json

import boto3
from dotenv import load_dotenv
from textractcaller.t_call import call_textract, Textract_Features
from textractprettyprinter.t_pretty_print import (Textract_Pretty_Print,
                                                  get_string)

load_dotenv()

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")

s3 = boto3.client('s3',
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key)

textract = boto3.client('textract',
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        region_name=aws_region)

def textract_extract_text(bucket, object_key):
    try:
        response = call_textract(input_document=f's3://{bucket}/{object_key}', boto3_textract_client=textract) 
        lines = get_string(textract_json=response, output_type=[Textract_Pretty_Print.LINES])
        
        # Extract patient information
        patient_info = extract_patient_info(lines)
        
        # Save patient information as JSON
        save_as_json(patient_info, f'{object_key}_patient_info.json')
        
        return patient_info
    except Exception as e:
        print(e)

def extract_patient_info(text):
    # Regex patterns for patient information
    phone_pattern = re.compile(r'\b(?:\(\d{3}\)\s*|\d{3}[-.\s]?)\d{3}[-.\s]??\d{4}\b')
    name_pattern = re.compile(r'\b(?:Mr\.|Mrs\.|Ms\.|Dr\.)?\s*[A-Z][a-z]*\s*[A-Z][a-z]*\b')
    
    phone_numbers = phone_pattern.findall(text)
    names = name_pattern.findall(text)
    
    patient_info = {
        "names": names,
        "phone_numbers": phone_numbers
    }
    
    return patient_info

def save_as_json(data, filename):
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=4)

def create_document_label(object_key, document_lines):
    label = ""
    # TODO: create label
    return label

def get_s3_bucket_objects(bucket):
    object_keys = []
    paginator = s3.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket}
    page_iterator = paginator.paginate(**operation_parameters)

    for page in page_iterator:
        for item in page['Contents']:
            object_keys.append(item['Key'])

    return object_keys

def get_s3_bucket_object(bucket):
    paginator = s3.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket}
    page_iterator = paginator.paginate(**operation_parameters)

    for page in page_iterator:
        if 'Contents' in page and page['Contents']:
            return page['Contents'][0]['Key']

def detect_signature(pngfile):
    from textractor.parsers import response_parser

    documentName = pngfile
    with open(documentName, 'rb') as document:
        imageBytes = bytearray(document.read())

    response = call_textract(input_document=imageBytes,
                              features=[Textract_Features.SIGNATURES])
    tdoc = response_parser.parse(response)

    for signature in tdoc.signatures:
        return signature.bbox, f"Confidence: {signature.confidence}\n"

# Example usage
print(textract_extract_text('capstone-intelligent-document-processing', '0008959634cb4bfd813f1193f8419ee9_OUT_PATIENT_2024_07_11_12_20_45_fd14dd8be98544faa22c85d26e19ed11_OTHER_Signed_Agreementpdf.null.pdf'))