import boto3
import os
from botocore.exceptions import ClientError


class S3Manager:
    """
    Object-oriented S3 helper class for uploads, downloads, listing, and bucket management.
    Automatically creates the bucket if it does not exist.
    Supports both file paths and file-like objects (e.g., Django UploadedFile or BytesIO).
    """

    def __init__(self, bucket_name=None, region_name="us-east-1"):
        # Ensure bucket_name is a valid string
        if not isinstance(bucket_name, str) or not bucket_name.strip():
            raise ValueError("S3Manager: A valid S3 bucket name (string) must be provided.")

        self.bucket_name = bucket_name.strip()
        self.region_name = region_name or "us-east-1"
        self.s3_client = boto3.client("s3", region_name=self.region_name)

        # Ensure the bucket exists automatically
        self._ensure_bucket_exists()

    # ---------------- Bucket Management ----------------
    def _ensure_bucket_exists(self):
        """Check if the S3 bucket exists; create if not."""
        try:
            existing_buckets = [b['Name'] for b in self.s3_client.list_buckets().get('Buckets', [])]
            if self.bucket_name not in existing_buckets:
                print(f"Bucket '{self.bucket_name}' not found. Creating it...")
                if self.region_name == "us-east-1":
                    self.s3_client.create_bucket(Bucket=self.bucket_name)
                else:
                    self.s3_client.create_bucket(
                        Bucket=self.bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': self.region_name}
                    )
                print(f"Bucket '{self.bucket_name}' created successfully.")
            # Remove or comment out this line:
            # else:
            #     print(f"Bucket '{self.bucket_name}' already exists.")
        except ClientError as e:
            print(f"Error ensuring bucket existence: {e}")
            raise

    # ---------------- Upload ----------------
    def upload_file(self, file_path, object_name=None):
        """Upload a local file by path"""
        object_name = object_name or os.path.basename(file_path)
        try:
            self.s3_client.upload_file(file_path, self.bucket_name, object_name)
            print(f"File uploaded to S3: {self.bucket_name}/{object_name}")
        except ClientError as e:
            print(f"Upload failed: {e}")
            raise

    def upload_fileobj(self, file_obj, object_name):
        """Upload an in-memory file (like BytesIO or Django UploadedFile)"""
        try:
            self.s3_client.upload_fileobj(file_obj, self.bucket_name, object_name)
            print(f"File object uploaded: {self.bucket_name}/{object_name}")
        except ClientError as e:
            print(f"File upload failed: {e}")
            raise

    # ---------------- Download ----------------
    def download_file(self, object_name, download_path):
        """Download a file from S3 to a local path"""
        try:
            self.s3_client.download_file(self.bucket_name, object_name, download_path)
            print(f"File downloaded: {self.bucket_name}/{object_name}")
        except ClientError as e:
            print(f"Download failed: {e}")
            raise

    # ---------------- List ----------------
    def list_objects(self, prefix=''):
        """List files inside this bucket safely"""
        try:
            if not isinstance(self.bucket_name, str) or not self.bucket_name.strip():
                raise ValueError("Invalid bucket name provided for list_objects().")

            prefix = prefix or ""
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            return [item['Key'] for item in response.get('Contents', [])]

        except Exception as e:
            print(f"S3 list error: {e}")
            return []

    # ---------------- Delete ----------------
    def delete_file(self, file_key):
        """Delete an object from S3"""
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=file_key)
            print(f"Deleted object: {self.bucket_name}/{file_key}")
        except ClientError as e:
            print(f"Error deleting object: {e}")
            raise