# test_connections.py
import psycopg2
import boto3
from config import config

def test_postgresql():
    """Test PostgreSQL connection"""
    try:
        conn = psycopg2.connect(
            host=config.DB_HOST,
            database=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD
        )
        print("PostgreSQL: Connected successfully")
        conn.close()
        return True
    except Exception as e:
        print(f"PostgreSQL: Connection failed - {e}")
        return False

def test_minio():
    """Test MinIO connection"""
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=config.S3_ENDPOINT,
            aws_access_key_id=config.S3_ACCESS_KEY,
            aws_secret_access_key=config.S3_SECRET_KEY,
            config=boto3.session.Config(signature_version='s3v4'),
            verify=False
        )
        s3_client.list_buckets()
        print("MinIO: Connected successfully")
        return True
    except Exception as e:
        print(f"MinIO: Connection failed - {e}")
        return False

if __name__ == "__main__":
    print("=== Testing Service Connections ===")
    pg_ok = test_postgresql()
    minio_ok = test_minio()
    
    if pg_ok and minio_ok:
        print("All services are connected!")
    else:
        print("Check if Docker services are running: docker-compose up -d")