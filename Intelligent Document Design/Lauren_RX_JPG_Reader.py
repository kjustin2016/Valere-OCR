import os
import boto3
import json
import re
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# AWS credentials and region
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")

# Initialize AWS clients
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

textract = boto3.client(
    'textract',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)

def textract_extract_text(bucket, object_key):
    """Extract text from JPG using Textract."""
    try:
        response = textract.analyze_document(
            Document={'S3Object': {'Bucket': bucket, 'Name': object_key}},
            FeatureTypes=["FORMS", "TABLES", "SIGNATURES"]
        )
        return response
    except Exception as e:
        print(f"Error extracting text with Textract: {e}")
        return None

def extract_fields_from_jpg(textract_response):
    """Extract specific fields from Textract response."""
    text_lines = [block['Text'] for block in textract_response.get('Blocks', []) if block['BlockType'] == 'LINE']
    full_text = "\n".join(text_lines)

    # Define regex patterns
    patterns = {
        "Name": r"(?i)\bname\b[:\s]*([^\n]+)",
        "Age/DOB": r"(?i)\b(dob|date of birth|age)\b[:\s]*([^\n]+)",
        "Address": r"(?i)\baddress\b[:\s]*([^\n]+)",
        "Date": r"(?i)\bdate\b[:\s]*([^\n]+)",
        "Rx": r"(?i)\brx\b[:\s]*([^\n]+)",
        "Refills": r"(?i)\brefills?\b[:\s]*([^\n]+)",
        "Signature": r"(?i)\bsignature\b[:\s]*([^\n]+)"
    }

    extracted_data = {}
    for field, pattern in patterns.items():
        match = re.search(pattern, full_text)
        if match:
            extracted_data[field] = match.group(2).strip() if field == "Age/DOB" else match.group(1).strip()
        else:
            extracted_data[field] = f"{field} not found"

    return extracted_data

def detect_signature(textract_response):
    """Detect signature presence from Textract blocks."""
    try:
        blocks = textract_response.get('Blocks', [])

        for block in blocks:
            if block.get('BlockType') == 'SIGNATURE':
                print("Signature detected via SIGNATURE block type")
                return True

        for block in blocks:
            if block.get('BlockType') == 'LINE':
                geometry = block.get('Geometry', {})
                bbox = geometry.get('BoundingBox', {})
                width = bbox.get('Width', 0)
                height = bbox.get('Height', 0)
                if width > 0.2 and height < 0.05 and 'Text' not in block:
                    print("Possible signature detected via geometry")
                    return True

        all_text = ' '.join([block.get('Text', '').lower() for block in blocks if 'Text' in block])
        for indicator in ["signature", "signed", "/s/"]:
            if indicator in all_text:
                print(f"Signature presence inferred from text: '{indicator}'")
                return True

        return False
    except Exception as e:
        print(f"Error during signature detection: {e}")
        return False

if __name__ == "__main__":
    # === SET YOUR BUCKET NAME AND JPG FILE NAME HERE ===
    bucket_name = "capstone-intelligent-document-processing"
    object_key = "11f7512841694ca6a64909e804c09d49_OUT_PATIENT_2024_09_22_15_27_15_1b143787b64043419441017b5f1b5ed3_PRESCRIPTION_imagejpg.null.jpg" 

    print(f"Processing file: {object_key} from bucket: {bucket_name}")

    textract_response = textract_extract_text(bucket_name, object_key)

    if textract_response:
        data = extract_fields_from_jpg(textract_response)
        data["Signature Present"] = "Yes" if detect_signature(textract_response) else "No"

        json_output = json.dumps(data, indent=4)
        print(f"\nExtracted Data:\n{json_output}")

        with open("jpg_extracted_info.json", "w") as f:
            f.write(json_output)
    else:
        print("Textract failed to process the image.")
