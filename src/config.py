#!/usr/bin/env python3
"""
Configuration for bank batch pipeline - FOR DOCKER CONTAINER USE
"""

# PostgreSQL configuration - for use INSIDE Docker container
DB_CONFIG = {
    'host': 'postgres',  # Docker service name
    'port': 5432,
    'database': 'bank_data',
    'user': 'admin',
    'password': 'password123'
}

# MinIO configuration - for use INSIDE Docker container  
MINIO_CONFIG = {
    'endpoint': 'minio:9000',  # Docker service name
    'access_key': 'admin',
    'secret_key': 'password123',
    'secure': False
}

# Data path
DATA_PATH = '/app/data'