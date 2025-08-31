import boto3
import hashlib
import os
from datetime import datetime

# Replace with your bucket name and region
bucket_name = "bucket-my-first"
region_name = "eu-north-1"

s3 = boto3.client('s3', region_name=region_name)

def hash_file(file_content):
    """Generate SHA256 hash for given file content"""
    return hashlib.sha256(file_content).hexdigest()

def remove_duplicates():
    response = s3.list_objects_v2(Bucket=bucket_name)

    if 'Contents' not in response:
        print("Bucket is empty.")
        return

    seen_hashes = {}
    duplicates = []
    total_files = 0

    for obj in response['Contents']:
        key = obj['Key']
        print(f"Scanning {key}...")
        total_files += 1

        file_obj = s3.get_object(Bucket=bucket_name, Key=key)
        file_content = file_obj['Body'].read()
        file_hash = hash_file(file_content)

        if file_hash in seen_hashes:
            # Duplicate found → delete it
            s3.delete_object(Bucket=bucket_name, Key=key)
            duplicates.append(key)
        else:
            seen_hashes[file_hash] = key

    # Generate report
    report_content = []
    report_content.append("===== Cleanup Summary =====")
    report_content.append(f"Date & Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_content.append(f"Total files scanned: {total_files}")
    report_content.append(f"Duplicates removed: {len(duplicates)}")
    report_content.append(f"Removed files: {duplicates if duplicates else 'None'}")

    # Save report locally
    with open("report.txt", "w") as f:
        f.write("\n".join(report_content))

    print("\n".join(report_content))
    print("\n✅ Report saved as report.txt")

if __name__ == "__main__":
    remove_duplicates()
