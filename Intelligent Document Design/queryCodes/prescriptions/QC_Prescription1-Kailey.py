import os
import boto3
import json
from dotenv import load_dotenv
from textractcaller.t_call import call_textract
from textractprettyprinter.t_pretty_print import (Textract_Pretty_Print, get_string)
import trp.trp2 as t2
from tabulate import tabulate

index_1 = 629

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

def get_s3_bucket_object_by_index(bucket, index=index_1):
    if index is None:  # Check if the index is None
        return None  # Skip the array index search and directly go to the entity tag

    paginator = s3.get_paginator('list_objects_v2')  # Paginate through the bucket
    operation_parameters = {'Bucket': bucket}
    page_iterator = paginator.paginate(**operation_parameters)

    # Loop through all pages and objects
    all_object_keys = []  # Store all object keys here for debugging purposes
    for page in page_iterator:
        if 'Contents' in page and page['Contents']:
            object_keys = [item['Key'] for item in page['Contents']]
            all_object_keys.extend(object_keys)  # Collect all object keys

    # Check if the index is valid
    if len(all_object_keys) > index:
        return all_object_keys[index]  # Return the object key at the given index
    else:
        print(f"Index {index} is out of range. Total objects available: {len(all_object_keys)}")
    return None  # Return None if index is out of bounds

response = textract.analyze_document(
        Document={'S3Object': {'Bucket': 'capstone-intelligent-document-processing', 'Name': get_s3_bucket_object_by_index('capstone-intelligent-document-processing', index_1)}},
        FeatureTypes=["QUERIES"],
        QueriesConfig={"Queries": [
            {"Text": "What is the Member Name", "Alias": "MEMBER_NAME"},
            {"Text": "What is the Memeber Sex?", "Alias": "MEMBER_SEX"},
            {"Text": "What is the Member DOB?", "Alias": "MEMBER_DOB"},
            {"Text": "What is the Member Phone?", "Alias": "MEMBER_PHONE"},
            {"Text": "What is the Member Age?", "Alias": "MEMBER_AGE"},
            {"Text": "What is the Member ID?", "Alias": "MEMBER_ID"},
            {"Text": "Who is the Presciber?", "Alias": "PRESCRIBER"},
            {"Text": "What is the phone number of the PCP?", "Alias": "PCP_PHONE"},
            {"Text": "What is the PCP Fax?", "Alias": "PCP_FAX"},
            {"Text": "What is the medical insurance provider?", "Alias": "MEDICAL_PROVIDER"},
            {"Text": "What is the Group Name?", "Alias": "GROUP_NAME"},
            {"Text": "What is the payer id?", "Alias": "PAYER_ID"},
            {"Text": "What is the Rx GRP?", "Alias": "RX_GRP"},
            {"Text": "What is the Applicable Diagnosis?", "Alias": "APPLICABLE_DIAGNOSIS"},
            {"Text": "What is the Supply?", "Alias": "SUPPLY"}
        ]}
)

response2 = textract.analyze_document(
        Document={'S3Object': {'Bucket': 'capstone-intelligent-document-processing', 'Name': get_s3_bucket_object_by_index('capstone-intelligent-document-processing', index_1)}},
        FeatureTypes=["QUERIES"],
        QueriesConfig={"Queries": [
        {"Text": "What is the Supply Quantity?", "Alias": "SUPPLY_QUANTITY"},
        {"Text": "What is the Supply Duration?", "Alias": "SUPPLY_DURATION"}
        ]}
)


d = t2.TDocumentSchema().load(response)
page = d.pages[0]
query_answers = d.get_query_answers(page=page)

d2 = t2.TDocumentSchema().load(response2)
page = d2.pages[0]
query_answers2 = d2.get_query_answers(page=page)

queryData={}

count=len(query_answers)
for i in range(count):
    (a,b,c) = query_answers[i]
    a = a.split("the ",1)[1]
    a = a.split("?",1)[0]
    queryData[a] = c

count2=len(query_answers2)
print(count2)
for i in range(count2):
    (a,b,c) = query_answers2[i]
    a = a.split("the ",1)[1]
    a = a.split("?",1)[0]
    queryData[a] = c

json_string = json.dumps(queryData, indent=4)
print(json_string)
