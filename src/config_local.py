#!/usr/bin/env python3
"""
Configuration for LOCAL testing (outside Docker)
"""

# PostgreSQL configuration - for LOCAL testing
DB_CONFIG = {
    'host': 'localhost',     # Localhost for outside Docker
    'port': 5432,
    'database': 'bank_data', # ACTUAL database name
    'user': 'admin',         # ACTUAL username
    'password': 'password123' # ACTUAL password
}

# MinIO configuration - for LOCAL testing  
MINIO_CONFIG = {
    'endpoint': 'localhost:9000', # Localhost for outside Docker
    'access_key': 'admin',        # ACTUAL access key
    'secret_key': 'password123',  # ACTUAL secret key
    'secure': False
}

# Data path
DATA_PATH = './data'