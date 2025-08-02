import os
import boto3
import json
from dotenv import load_dotenv
from textractcaller.t_call import call_textract
from textractprettyprinter.t_pretty_print import (Textract_Pretty_Print, get_string)
import trp.trp2 as t2
from tabulate import tabulate

index_1 = 63
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
        {"Text": "What is the Client Name?", "Alias": "clientname"},
        {"Text": "What is the Medicaid Number?", "Alias": "medicaid"},
        {"Text": "What is the Rendering Provider Name?", "Alias": "rendname"},
        {"Text": "What is the Rendering Provider Telephone Number?", "Alias": "renderphone"},
        {"Text": "What is the Rendering Provider Fax Number?", "Alias": "rendfax"},
        {"Text": "What is the Rendering Provider NPI?", "Alias": "npi"},
        {"Text": "What is the Rendering Provider Tax ID?", "Alias": "rendtaxid"},
        {"Text": "What is the Rendering Provider Taxonomy?", "Alias": "rendtax"},
        {"Text": "What is the Requesting Physician Name?", "Alias": "doctorname"},
        {"Text": "What is the Description of DME/Medical Supplies?", "Alias": "medsupply"},
        {"Text": "What is the Qty?", "Alias": "qty"},
        {"Text": "What is the HCPCS Code?", "Alias": "code"}

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

# print(tabulate(query_answers, tablefmt="github"))
# print(query_answers)

count=len(query_answers)
# (a,b,c), (d,e,f) = query_answers
# print(a,c,d,f)
# print(count)

queryData={}

for i in range(count):
    (a,b,c) = query_answers[i]
    a = a.split("the ",1)[1]
    a = a.split("?",1)[0]
    queryData[a] = c

# print(queryData)

# for answerText in query_answers:
#     print(answerText)

json_string = json.dumps(queryData, indent=4)
print(json_string)

# COMMENT OUT EVERYTHING BELOW THIS LINE (CTRL+/) if not using queries below
# response2 = textract.analyze_document(
#         Document={'S3Object': {'Bucket': 'capstone-intelligent-document-processing', 'Name': get_s3_bucket_object_by_index('capstone-intelligent-document-processing', index_1)}},
#         FeatureTypes=["QUERIES"],
#         QueriesConfig={"Queries": [
#         {"Text": "What is the Specialist Copay?", "Alias": "SPECIALIST_COPAY"},
#         ]}
# )

# d2 = t2.TDocumentSchema().load(response2)
# page = d2.pages[0]

# query_answers2 = d2.get_query_answers(page=page)
# print(tabulate(query_answers2, tablefmt="github"))
