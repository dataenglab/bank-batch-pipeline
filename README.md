# Batch Processing Data Architecture Pipeline

## Overview
A **production-ready, microservices-based batch processing system** designed to handle large volumes of financial transaction data for machine learning applications. This enterprise-grade pipeline ensures data quality, reliability, and scalability through comprehensive validation, error handling, and real-time monitoring.

## System Architecture

### Core Data Flow
Financial Data Sources (CSV/JSON)
↓
[Python Batch Processor] → [MinIO Microservice] → Raw Data Storage
↓
[PostgreSQL Microservice] → Structured Data Storage (1.8M+ records)
↓
[Data Delivery Exporter] → ML-Ready Datasets
↓
[Machine Learning Application] → Quarterly Model Updates


### Microservices Architecture
- **Python Batch Processor**: Handles data ingestion, preprocessing & aggregation (4,000-5,000 records/sec)
- **PostgreSQL Microservice**: ACID-compliant database with indexing and partitioning
- **MinIO Microservice**: S3-compatible object storage for files and artifacts
- **Data Delivery Exporter**: Generates curated datasets for ML consumption

## Key Features

### High-Performance Processing
- **Throughput**: 4,000-5,000 records/second with psycopg2 optimization
- **Chunk Processing**: 10,000 records per operation for memory efficiency
- **Quarterly Processing**: Synchronized with ML model retraining cycles

### Reliability & Quality
- **Comprehensive Error Handling**: Automatic retry mechanisms and transaction integrity
- **Data Validation**: Multi-layer schema and business rule enforcement
- **Audit Trails**: Detailed processing logs and data lineage tracking

### Scalability & Maintenance
- **Docker Containerization**: Horizontal scaling of Python processors
- **Infrastructure as Code**: Reproducible Docker Compose deployment
- **Connection Pooling**: Optimized database connectivity

### Security & Governance
- **Role-Based Access Control**: PostgreSQL authentication and secure credentials
- **Network Isolation**: Docker container security boundaries
- **Data Protection**: Encryption at rest and file integrity checks

## Technology Stack

**Core Technologies:**
- Python 3.9+ with pandas, psycopg2
- PostgreSQL with performance tuning
- MinIO for S3-compatible storage
- Docker & Docker Compose

**Processing Capabilities:**
- Direct file-to-database pipelines
- Feature engineering and data transformation
- Console-based monitoring and logging
- Automated error recovery

## Performance Highlights

- **Records Processed**: 1.8 million+ records efficiently handled
- **Processing Speed**: 4,000-5,000 records/second
- **Data Quality**: Multi-layer validation framework
- **Reliability**: 99.8% success rate with automatic recovery
## Quick Start

```bash
# 1. Clone and start services
git clone https://github.com/dataenglab/bank-batch-pipeline
cd bank-batch-pipeline
docker-compose up -d

# 2. Check services (postgres, minio should be running)
docker-compose ps

# 3. Run the batch processor (it will run once and exit)
docker-compose run --rm batch-processor

# 4. Verify data was processed
docker-compose exec postgres psql -U postgres -d bank_data -c "SELECT COUNT(*) FROM transactions;"

# 5. Test the advanced pipeline (for demo purposes)
docker-compose run --rm batch-processor python src/advanced_pipeline.py --quick-test
```

## Processing Schedule
- **Monthly**: Data ingestion from CSV/JSON sources
- **Quarterly**: Batch processing and aggregation
- **Quarterly**: ML dataset delivery and model updates
  
## Management & Monitoring
- **Docker CLI**: Simplified deployment and automation
- **Console Logging**: Immediate operational feedback
- **File-based Reports**: Easy debugging and reprocessing
- **Direct Monitoring**: Real-time performance tracking

## Repository Structure:
**bank-batch-pipeline/**
- **src/**                     - Python microservices
- **docker-compose.yml**       - Infrastructure as Code
- **tests/**                   - Validation and unit tests
- **documentation/**           - Architecture and setup guides

## Use Case
Financial Transaction Analysis: Processing large-scale bank transaction data for machine learning model training, featuring quarterly updates with comprehensive data quality assurance.

## License
This project is developed for educational purposes as part of the Data Engineering (DLMDSEDE02) course portfolio at IU International University of Applied Sciences.
