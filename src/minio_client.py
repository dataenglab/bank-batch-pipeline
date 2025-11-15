from minio import Minio
import os

class MinioClient:
    def __init__(self):
        self.client = Minio(
            os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            access_key=os.getenv('MINIO_ACCESS_KEY', 'admin'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'password123'),
            secure=False
        )
    
    def upload_data(self, data, object_name, bucket_name, length):
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
            
            self.client.put_object(
                bucket_name, object_name, data, length
            )
            return True
        except Exception as e:
            print(f"MinIO upload error: {e}")
            return False
