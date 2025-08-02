import os
import boto3
import json
from dotenv import load_dotenv
import trp.trp2 as t2
from tabulate import tabulate
import fitz  # PyMuPDF for PDF processing

index_1 = 3
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

def extract_last_lines_from_pdf(pdf_path, num_lines=10):
    # Open the PDF file
    doc = fitz.open(pdf_path)
    text = ''
    
    # Iterate through the pages starting from the last page
    for page_num in range(doc.page_count - 1, -1, -1):
        page = doc.load_page(page_num)
        text = page.get_text("text") + text  # Add the text from the current page at the top
        
        # If we've collected enough lines, stop processing
        if text.count("\n") >= num_lines:
            break
    
    # Return the last num_lines lines of text
    lines = text.split("\n")[-num_lines:]
    return "\n".join(lines)

# Check that the file is a PDF before proceeding
object_key = get_s3_bucket_object_by_index('capstone-intelligent-document-processing', index_1)
if object_key.endswith('.pdf'):  # Ensure the document is a PDF
    # Download the file locally from S3
    s3.download_file('capstone-intelligent-document-processing', object_key, '/tmp/temp.pdf')
    
    # Extract the last 10 lines from the PDF
    last_lines = extract_last_lines_from_pdf('/tmp/temp.pdf', num_lines=10)
    
    # Now, submit only the extracted text as a query to Textract
    response = textract.start_document_analysis(
        DocumentLocation={'S3Object': {'Bucket': 'capstone-intelligent-document-processing', 'Name': object_key}},
        FeatureTypes=["QUERIES"],
        QueriesConfig={
            "Queries": [
                {"Text": "What is the signed by customer name?", "Alias": "clientname"},
                {"Text": "What is the date?", "Alias": "medicaid"}
            ]
        }
    )

    # Get the JobId from the response
    job_id = response['JobId']

    # Poll for the result of the document analysis job
    while True:
        result = textract.get_document_analysis(JobId=job_id)
        status = result['JobStatus']

        if status in ['SUCCEEDED', 'FAILED']:
            break

    if status == 'SUCCEEDED':
        # Process the result
        d = t2.TDocumentSchema().load(result)
        page = d.pages[0]
        query_answers = d.get_query_answers(page=page)
        print(tabulate(query_answers, tablefmt="github"))
    else:
        print("Document analysis failed.")
else:
    print(f"The document {object_key} is not a PDF or supported file type.")
