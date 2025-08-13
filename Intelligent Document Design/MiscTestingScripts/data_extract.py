import os

import boto3
from dotenv import load_dotenv
from textractcaller.t_call import call_textract, Textract_Features
from textractprettyprinter.t_pretty_print import (Textract_Pretty_Print,
                                                  get_string)

load_dotenv()

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")

s3 = boto3.client('s3',#creates boto client for s3
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key)

textract = boto3.client('textract',
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        region_name=aws_region)

def textract_extract_text(bucket, object_key): #one of the document file name (key)
    try:
        row = []#creates list
        response = call_textract(input_document=f's3://{bucket}/{object_key}', boto3_textract_client=textract) 
        lines = get_string(textract_json=response, output_type=[Textract_Pretty_Print.LINES])
        row.append(create_document_label(object_key, lines))
        row.append(lines)
        return row
    except Exception as e:
        print (e)

def create_document_label(object_key, document_lines):
    label = ""
    # TODO: create label
    return label

def get_s3_bucket_objects(bucket):
    object_keys = []
    paginator = s3.get_paginator('list_objects_v2')#function to go page by page
    operation_parameters = {'Bucket': bucket}
    page_iterator = paginator.paginate(**operation_parameters)

    for page in page_iterator:
        for item in page['Contents']:
            object_keys.append(item['Key'])

    return object_keys

def get_s3_bucket_object(bucket):#return only one object key
    object_keys = []
    paginator = s3.get_paginator('list_objects_v2')#function to go page by page
    operation_parameters = {'Bucket': bucket}
    page_iterator = paginator.paginate(**operation_parameters)

    for page in page_iterator:
        if 'Contents' in page and page['Contents']:
            return page['Contents'][0]['Key']

def detect_signature(pngfile):
    from textractor.parsers import response_parser

    documentName=pngfile
    with open(documentName, 'rb') as document:
        imageBytes = bytearray(document.read())

# Call Amazon Textract
    response = call_textract(input_document=imageBytes,
                              features=[Textract_Features.SIGNATURES])
    tdoc = response_parser.parse(response)

    for signature in tdoc.signatures:
        return signature.bbox, f"Confidence: {signature.confidence}\n"

    
# print(detect_signature('theone.png'))

# print(get_s3_bucket_objects('capstone-intelligent-document-processing'))#the capstone bucket
#creates a list, goes through every document and prints the list of all object keys (document names)

# print(get_s3_bucket_object('capstone-intelligent-document-processing'))
#gets the object key from only the first document

print(textract_extract_text('capstone-intelligent-document-processing', '0008959634cb4bfd813f1193f8419ee9_OUT_PATIENT_2024_07_11_12_20_45_fd14dd8be98544faa22c85d26e19ed11_OTHER_Signed_Agreementpdf.null.pdf'))
#reads the first document
