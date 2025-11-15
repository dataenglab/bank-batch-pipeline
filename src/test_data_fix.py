import pandas as pd
from enhanced_processor import EnhancedBatchProcessor
import psycopg2
import boto3

def create_test_data():
    """Create sample bank transaction data for testing"""
    return pd.DataFrame({
        'transaction_id': ['TXN001', 'TXN002', 'TXN003', 'TXN004'],
        'account_number': ['ACC123456', 'ACC789012', 'ACC345678', 'ACC901234'],
        'transaction_date': ['15/10/63', '20/11/64', '25/12/65', '30/01/66'],
        'amount': ['1000.50', '2500.75', '-500.25', '3000.00'],
        'description': ['Salary Credit', 'Fund Transfer', 'ATM Withdrawal', 'Online Payment'],
        'transaction_type': ['CREDIT', 'CREDIT', 'DEBIT', 'DEBIT'],
        'branch_code': ['BR001', 'BR002', 'BR001', 'BR003']
    })

def setup_connections():
    """Setup database and storage connections using config"""
    from config import config
    
    # PostgreSQL connection
    pg_conn = psycopg2.connect(
        host=config.DB_HOST,
        database=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD
    )
    
    # MinIO/S3 connection
    s3_client = boto3.client(
        's3',
        endpoint_url=config.S3_ENDPOINT,
        aws_access_key_id=config.S3_ACCESS_KEY,
        aws_secret_access_key=config.S3_SECRET_KEY,
        aws_session_token=None,
        config=boto3.session.Config(signature_version='s3v4'),
        verify=False
    )
    
    return pg_conn, s3_client

def main():
    """Main test function"""
    print("=== Starting Bank Batch Pipeline Test ===")
    
    # Setup connections
    pg_conn, s3_client = setup_connections()
    
    # Initialize processor
    processor = EnhancedBatchProcessor(pg_conn, s3_client)
    
    # Test connections
    processor.test_connections()
    
    # Setup storage
    processor.setup_storage()
    
    # Create test data
    test_data = create_test_data()
    print(f"Created test data with {len(test_data)} records")
    
    # Process batch
    results = processor.process_batch(test_data)
    
    # Generate report
    report = processor.generate_report(results)
    print(report)
    
    # Cleanup
    processor.close_connections()
    print("Test completed!")

if __name__ == "__main__":
    main()