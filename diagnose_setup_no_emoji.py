#!/usr/bin/env python3
"""
Comprehensive diagnostic script for the bank batch pipeline
"""

import os
import subprocess
import psycopg2

def check_project_structure():
    print("PROJECT STRUCTURE")
    print("=" * 50)
    
    # Check essential files
    essential_files = [
        'docker-compose.yml',
        'Dockerfile',
        'requirements.txt',
        'src/',
        'data/'
    ]
    
    for file_path in essential_files:
        if os.path.exists(file_path):
            print(f"FOUND: {file_path}")
            if file_path == 'docker-compose.yml':
                with open(file_path, 'r') as f:
                    content = f.read()
                    if 'postgres' in content.lower():
                        print("   - Contains PostgreSQL service")
                    if 'minio' in content.lower():
                        print("   - Contains MinIO service")
        else:
            print(f"MISSING: {file_path}")

def check_docker_services():
    print("\nDOCKER SERVICES")
    print("=" * 50)
    
    try:
        # Check running services
        result = subprocess.run(['docker-compose', 'ps'], capture_output=True, text=True)
        print("Running services:")
        print(result.stdout)
        
    except Exception as e:
        print(f"Error checking Docker: {e}")

def check_postgres_connection():
    print("\nPOSTGRESQL CONNECTION")
    print("=" * 50)
    
    # Common connection attempts
    attempts = [
        # Standard Docker Compose
        {'host': 'localhost', 'port': 5432, 'user': 'postgres', 'password': 'postgres', 'database': 'postgres'},
        {'host': 'localhost', 'port': 5432, 'user': 'postgres', 'password': 'postgres', 'database': 'bank_transactions'},
        # Container name as host
        {'host': 'postgres', 'port': 5432, 'user': 'postgres', 'password': 'postgres', 'database': 'postgres'},
        {'host': 'bank-batch-pipeline-postgres-1', 'port': 5432, 'user': 'postgres', 'password': 'postgres', 'database': 'postgres'},
        # No password
        {'host': 'localhost', 'port': 5432, 'user': 'postgres', 'password': '', 'database': 'postgres'},
    ]
    
    for attempt in attempts:
        try:
            print(f"Trying: {attempt['user']}@{attempt['host']}:{attempt['port']}/{attempt['database']}")
            conn = psycopg2.connect(**attempt)
            cursor = conn.cursor()
            
            # Get connection info
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            print(f"SUCCESS: {version.split(',')[0]}")
            
            conn.close()
            return attempt
            
        except Exception as e:
            print(f"FAILED: {e}")
    
    print("Could not connect to PostgreSQL")
    return None

def check_data_files():
    print("\nDATA FILES")
    print("=" * 50)
    
    data_dir = 'data'
    if os.path.exists(data_dir):
        files = os.listdir(data_dir)
        print(f"Files in data directory: {files}")
        
        if 'bank_transactions.csv' in files:
            file_path = os.path.join(data_dir, 'bank_transactions.csv')
            file_size = os.path.getsize(file_path)
            print(f"bank_transactions.csv exists ({file_size:,} bytes)")
        else:
            print("bank_transactions.csv not found")
    else:
        print("data directory not found")

if __name__ == "__main__":
    check_project_structure()
    check_docker_services()
    working_config = check_postgres_connection()
    check_data_files()
    
    if working_config:
        print(f"\nWORKING CONFIGURATION:")
        print(f"   host: {working_config['host']}")
        print(f"   port: {working_config['port']}")
        print(f"   database: {working_config['database']}")
        print(f"   user: {working_config['user']}")
        print(f"   password: {working_config['password']}")