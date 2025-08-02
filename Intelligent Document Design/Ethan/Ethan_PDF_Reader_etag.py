import os
import boto3
import json
import time
import re
import PyPDF2
from io import BytesIO
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize AWS clients
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")

s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
textract = boto3.client('textract', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=aws_region)

# Define the entity tag to search for
entity_tag = "24eb56b8e0742c7b8637746517198223"  # Replace with the actual entity tag

def get_s3_bucket_object_by_tag(bucket, tag):
    paginator = s3.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket}
    page_iterator = paginator.paginate(**operation_parameters)

    for page in page_iterator:
        if 'Contents' in page and page['Contents']:
            for item in page['Contents']:
                head_object = s3.head_object(Bucket=bucket, Key=item['Key'])
                if 'ETag' in head_object and head_object['ETag'].strip('"') == tag:
                    return item['Key']
    
    print(f"No object found with tag: {tag}")
    return None

def extract_text_from_pdf(bucket, object_key):
    """Extract text directly from PDF using PyPDF2"""
    try:
        # Download the PDF file from S3
        response = s3.get_object(Bucket=bucket, Key=object_key)
        pdf_content = response['Body'].read()
        
        # Process the PDF
        pdf_file = BytesIO(pdf_content)
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        
        # Extract text from all pages
        all_text = ""
        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            all_text += page.extract_text() + "\n\n"
        
        return all_text
    except Exception as e:
        print(f"Error extracting text from PDF: {e}")
        return None

def textract_extract_text(bucket, object_key):
    try:
        # Check file extension
        file_extension = object_key.split('.')[-1].lower()

        if file_extension in ['png', 'jpg', 'jpeg']:
            response = textract.analyze_document(
                Document={'S3Object': {'Bucket': bucket, 'Name': object_key}},
                FeatureTypes=["FORMS", "TABLES", "SIGNATURES"]
            )
        elif file_extension == 'pdf':
            response = textract.start_document_analysis(
                DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': object_key}},
                FeatureTypes=["FORMS", "TABLES", "SIGNATURES"]
            )
            job_id = response['JobId']
            print(f"Started asynchronous job for PDF with JobId: {job_id}")

            while True:
                result = textract.get_document_analysis(JobId=job_id)
                status = result['JobStatus']
                if status in ['SUCCEEDED', 'FAILED']:
                    break
                print("Waiting for Textract to complete the analysis...")
                time.sleep(5)

            if status == 'SUCCEEDED':
                response = result
            else:
                raise ValueError("Textract document analysis failed.")
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")

        if response is None:
            raise ValueError("Textract response is None")

        return response

    except Exception as e:
        print(f"Error in textract_extract_text: {e}")
        return None

def extract_information_signed_agreement(response, pdf_text):
    agreement_info = {
        "Customer/Patient Name": "No name found",
        "Date": "No date found",
        "Signature Present": "No"
    }
    
    # Extract date from filename
    global object_key
    if 'object_key' in globals():
        date_match = re.search(r"(\d{4}_\d{2}_\d{2})", object_key)
        if date_match:
            date_str = date_match.group(1).replace('_', '/')
            agreement_info["Date"] = date_str
            print(f"Extracted date from filename: {date_str}")
    
    # Try to extract customer name from the PDF text
    if pdf_text:
        # Save PDF text for manual review
        with open("full_pdf_text.txt", "w", encoding="utf-8") as f:
            f.write(pdf_text)
        
        # Look for "Signed by customer:" pattern
        name_patterns = [
            r"Signed by customer\s*:\s*([^\n\.;,]+)",
            r"Signed by\s*:\s*([^\n\.;,]+)",
            r"Customer\s*:\s*([^\n\.;,]+)"
        ]
        
        for pattern in name_patterns:
            matches = re.findall(pattern, pdf_text, re.IGNORECASE)
            if matches:
                for match in matches:
                    name = match.strip()
                    if len(name) > 2 and len(name) < 50:  # Reasonable name length
                        agreement_info["Customer/Patient Name"] = name
                        print(f"Found customer name from PDF: {name}")
                        break
                if agreement_info["Customer/Patient Name"] != "No name found":
                    break
    
    # If no name found, try once more with a broader approach
    if agreement_info["Customer/Patient Name"] == "No name found" and pdf_text:
        # Find lines containing "signed" or "customer"
        lines = pdf_text.split('\n')
        for i, line in enumerate(lines):
            if ("signed" in line.lower() or "customer" in line.lower()) and ":" in line:
                print(f"Potential signature line: {line}")
                parts = line.split(":", 1)
                if len(parts) > 1:
                    name = parts[1].strip()
                    if name and len(name) > 2:
                        agreement_info["Customer/Patient Name"] = name
                        print(f"Found customer name from line: {name}")
                        break
    
    # Try to extract date from the PDF text
    date_patterns = [
        r"Date\s*:\s*(\d{2}/\d{2}/\d{4})",
        r"Fecha\s*:\s*(\d{2}/\d{2}/\d{4})",
        r"(\d{2}/\d{2}/\d{4})"
    ]
    
    for pattern in date_patterns:
        matches = re.findall(pattern, pdf_text, re.IGNORECASE)
        if matches:
            for match in matches:
                date = match.strip()
                if len(date) == 10:  # Reasonable date length
                    agreement_info["Date"] = date
                    print(f"Found date from PDF: {date}")
                    break
            if agreement_info["Date"] != "No date found":
                break
    
    return agreement_info

def detect_signature(response):
    try:
        blocks = response.get('Blocks', [])
        
        # Method 1: Check for SIGNATURE block type
        for block in blocks:
            if block.get('BlockType') == 'SIGNATURE':
                print(f"Signature detected via SIGNATURE block type")
                return True
                
        # Method 2: Look for lines that might be signatures
        for block in blocks:
            if block.get('BlockType') == 'LINE':
                if 'Geometry' in block:
                    geometry = block.get('Geometry', {})
                    bbox = geometry.get('BoundingBox', {})
                    width = bbox.get('Width', 0)
                    height = bbox.get('Height', 0)
                    
                    if width > 0.2 and height < 0.05 and 'Text' not in block:
                        print(f"Possible signature detected via geometry analysis")
                        return True
        
        # Method 3: Check for specific text indicators
        all_text = ' '.join([block.get('Text', '').lower() for block in blocks if 'Text' in block])
        signature_indicators = ["signature", "signed", "/s/"]
        for indicator in signature_indicators:
            if indicator in all_text:
                print(f"Signature presence inferred from text: '{indicator}'")
                return True
                
        print("No signatures detected.")
        return False
        
    except Exception as e:
        print(f"Error in signature detection: {e}")
        return False

if __name__ == "__main__":
    bucket_name = "capstone-intelligent-document-processing"
    print(f"Bucket name: {bucket_name}")
    
    # Use the entity tag to get the object key
    object_key = get_s3_bucket_object_by_tag(bucket_name, entity_tag)
    print(f"Object key: {object_key}")
    
    if object_key:
        # Get Textract analysis for signature detection
        textract_response = textract_extract_text(bucket_name, object_key)
        
        # Extract text directly from PDF
        pdf_text = extract_text_from_pdf(bucket_name, object_key)
        
        if textract_response and pdf_text:
            agreement_info = extract_information_signed_agreement(textract_response, pdf_text)
            signature_present = detect_signature(textract_response)
            agreement_info["Signature Present"] = "Yes" if signature_present else "No"
            
            # Format as JSON
            json_output = json.dumps(agreement_info, indent=4)
            print(f"Extracted Information in JSON format:\n{json_output}")
            
            # Save to file
            with open("extracted_info.json", "w") as json_file:
                json_file.write(json_output)
        else:
            print("Failed to extract data from document.")
    else:
        print("No documents found in the S3 bucket.")