import os
import boto3
from dotenv import load_dotenv

load_dotenv()

# Config
LOGS_DIR = os.path.expanduser(
    "~/Desktop/code_basics/project-care-plus/data-ingestion/support-logs/day-wise-logs-data/"
)
BUCKET_NAME = "careplus-pipeline-seyi"
S3_PREFIX = "logs/"

def upload_logs_to_s3():
    s3 = boto3.client("s3")
    
    log_files = sorted([
        f for f in os.listdir(LOGS_DIR)
        if f.startswith("support_logs_2025-07-") and f.endswith(".log")
    ])

    if not log_files:
        print("No log files found. Check your directory path.")
        return

    print(f"Found {len(log_files)} log files. Starting upload...\n")

    for filename in log_files:
        local_path = os.path.join(LOGS_DIR, filename)
        s3_key = f"{S3_PREFIX}{filename}"

        try:
            s3.upload_file(local_path, BUCKET_NAME, s3_key)
            print(f"✅ Uploaded: {filename}")
        except Exception as e:
            print(f"❌ Failed: {filename} — {e}")

    print("\nUpload complete.")

if __name__ == "__main__":
    upload_logs_to_s3()