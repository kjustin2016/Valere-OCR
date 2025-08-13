import os
import boto3
import json
from dotenv import load_dotenv
from textractcaller.t_call import call_textract
from textractprettyprinter.t_pretty_print import (Textract_Pretty_Print, get_string)
import trp.trp2 as t2
from tabulate import tabulate

index_1 = 107
# Place array index of S3 object here

# for x in query_answers:
#     print(f"{s3_object_name},{x[1]},{x[2]}")

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
        {"Text": "What is the Patient Name?", "Alias": "patientname"},
        {"Text": "What is the Patient Date of Birth?", "Alias": "patientdob"},
        {"Text": "What is the Patient Address?", "Alias": "patientaddress"},
        {"Text": "What is the Patient sex?", "Alias": "patientsex"},
        {"Text": "What is the Patient Ethnicity?", "Alias": "patientethnicity"},
        {"Text": "What is the Patient citizenship?", "Alias": "patientcitizenship"},
        {"Text": "What is the Patient Race?", "Alias": "patientrace"},
        {"Text": "What is the Patient Phone Number?", "Alias": "patientphone"},
        {"Text": "What is the Admitting Provider Name?", "Alias": "admittingname"},
        {"Text": "What is the Attending Provider Telephone Number?", "Alias": "attendphone"},
        {"Text": "What is the Attending Provider Name?", "Alias": "attendname"},
        {"Text": "What is the Refering physician?", "Alias": "refphysician"},
        {"Text": "What is the admitting diagnosis?", "Alias": "admittingdiagnosis"},
        {"Text": "What is the Encounter Date?", "Alias": "encounterdate"},
        {"Text": "What is the MRN?", "Alias": "mrn"}
        
        

        # Delete and change queries as needed
        # make sure there are commas in between, and NO commas at the end of the list
        # Do not add more queries, add more queries to the next section that looks like this.
        # The cap per request is 15 queries, so it will throw an error if you do add more than 15
        # COMMENT OUT BELOW WHERE IT SAYS TO if not using more queries
        ]}
)

d = t2.TDocumentSchema().load(response)
page = d.pages[0]

query_answers = d.get_query_answers(page=page)
# for x in query_answers:
#     print(f"{s3_object_name},{x[1]},{x[2]}")

# COMMENT OUT EVERYTHING BELOW THIS LINE (CTRL+/) if not using queries below
response2 = textract.analyze_document(
        Document={'S3Object': {'Bucket': 'capstone-intelligent-document-processing', 'Name': get_s3_bucket_object_by_index('capstone-intelligent-document-processing', index_1)}},
        FeatureTypes=["QUERIES"],
        QueriesConfig={"Queries": [
        {"Text": "What is the Hospital Account number?", "Alias": "hospitalaccountnumber"},
        {"Text": "What is the Contact Serial number?", "Alias": "contactserialnumber"},
        {"Text": "What is the Patient insurance provider?", "Alias": "patientinsuranceprovider"},
        {"Text": "What is the insurance Subscriber name?", "Alias": "insurancesubscribername"},
        {"Text": "What is the Patient insurance group number?", "Alias": "patientinsurancegroupnumber"},
        {"Text": "What is the Patient insurance Subscriber Id?", "Alias": "patientinsurancesubscriberid"},
        {"Text": "What is the Patient insurance type?", "Alias": "patientinsurancetype"},
        {"Text": "What is the Patient insurance plan?", "Alias": "patientinsuranceplan"},
        {"Text": "What is the Patient relationship to insurance Subscriber ?", "Alias": "patientrelationshiptoinsurancesubscriber"},
        {"Text": "What is the insurance verifiaction status?", "Alias": "insuranceverificationstatus"},
        {"Text": "What is the Garuntor Name?", "Alias": "garuntorname"},
        {"Text": "What is the Garuntor relation to patient?", "Alias": "garuntorrelationtopatient"},
        {"Text": "What is the Garuntor Id?", "Alias": "garuntorid"},
        {"Text": "What is the Garuntor Address?", "Alias": "garuntoraddress"},
        {"Text": "What is the Garuntor Phone number?", "Alias": "garuntorphone"}
        ]}
)

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