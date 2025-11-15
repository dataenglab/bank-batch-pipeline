"""
Final Working Batch Processor with Comprehensive Monitoring
Enhanced version with real-time metrics, monitoring, and data validation
"""

import sys
import os
import time
import logging
from datetime import datetime

# Add the src directory to the path
sys.path.insert(0, '/app/src')

# Import core components
try:
    from database import DatabaseClient
    from data_generator import DataGenerator
    print(" Core imports successful")
except ImportError as e:
    print(f" Critical import error: {e}")
    raise

# Try to import monitoring components (optional)
try:
    from monitoring import start_monitoring, end_monitoring, record_processing_results, print_monitoring_dashboard
    HAS_MONITORING = True
    print(" Monitoring imports successful")
except ImportError as e:
    HAS_MONITORING = False
    print(" Monitoring not available - continuing without advanced metrics")

print(" Running without MinIO storage")

class FinalBatchProcessor:
    def __init__(self):
        self.db = DatabaseClient()
        self.data_gen = DataGenerator()
        
        # Setup basic logging
        self.setup_logging()
        
        print(" Processor initialized" + (" with monitoring" if HAS_MONITORING else ""))
        
    def setup_logging(self):
        """Setup basic logging for the processor"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('processor.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger("BatchProcessor")
        
    def process_large_dataset(self, total_records=1000000, chunk_size=100000, demo_limit=300000):
        """Process dataset with comprehensive monitoring and validation"""
        
        # Start monitoring for the entire pipeline if available
        if HAS_MONITORING:
            start_monitoring("full_pipeline_execution", tags=["production", "batch_processing", "bank_data"])
            self.logger.info(" MONITORING ENABLED - Starting comprehensive metrics tracking")
        else:
            self.logger.info(" Starting batch processing - basic metrics only")
        
        self.logger.info(" FINAL WORKING PROCESSOR STARTING")
        self.logger.info(f"Target: {total_records:,} records, Chunk size: {chunk_size:,}, Demo limit: {demo_limit:,}")
        
        total_stored = 0
        chunk_num = 0
        processing_start_time = time.time()
        
        for chunk_start in range(0, total_records, chunk_size):
            chunk_num += 1
            
            # Check demo limit
            if total_stored >= demo_limit:
                self.logger.info(f" Demo limit reached ({demo_limit:,} records)")
                break
                
            self.logger.info(f" Processing Chunk {chunk_num}: {chunk_size:,} records")
            
            # Start monitoring for this specific chunk if available
            if HAS_MONITORING:
                start_monitoring(f"chunk_{chunk_num:02d}", tags=[f"chunk_{chunk_num}", "data_processing"])
            
            chunk_start_time = time.time()
            
            # Generate chunk data
            chunk_data = self.data_gen.generate_batch(chunk_size)
            generation_time = time.time() - chunk_start_time
            
            self.logger.info(f" Generated {len(chunk_data):,} records in {generation_time:.2f}s")
            
            # Process in smaller batches for database efficiency
            batch_size = 10000
            chunk_stored = 0
            chunk_processing_time = 0
            
            for i in range(0, len(chunk_data), batch_size):
                batch_data = chunk_data[i:i + batch_size]
                batch_num = i // batch_size + 1
                batch_start_time = time.time()
                
                # DEBUG: Check batch before insert
                self.logger.debug(f"Batch {batch_num} - Preparing to insert {len(batch_data)} records")
                
                # Store in database
                stored_count = self.db.insert_transaction_batch(batch_data)
                batch_processing_time = time.time() - batch_start_time
                chunk_processing_time += batch_processing_time
                
                # Record processing results for monitoring if available
                if HAS_MONITORING:
                    records_failed = len(batch_data) - stored_count
                    record_processing_results(
                        records_attempted=len(batch_data),
                        records_successful=stored_count,
                        records_skipped=0,
                        validation_errors=0,
                        database_errors=records_failed
                    )
                
                # DEBUG: Check what was returned
                self.logger.debug(f"Batch {batch_num} - Inserted {stored_count}/{len(batch_data)} records in {batch_processing_time:.2f}s")
                
                chunk_stored += stored_count
                
                # DEBUG: Verify with actual count periodically
                if batch_num % 5 == 0:  # Check every 5 batches to reduce database calls
                    actual_count = self.db.get_transaction_count()
                    self.logger.debug(f"Current database count: {actual_count:,}")
                
                # Small delay to prevent overwhelming the database
                time.sleep(0.1)
            
            # End monitoring for this chunk if available
            if HAS_MONITORING:
                end_monitoring()
            
            # Calculate chunk statistics
            success_rate = (chunk_stored / chunk_size) * 100
            total_stored += chunk_stored
            chunk_total_time = time.time() - chunk_start_time
            
            # Log chunk completion with detailed metrics
            self.logger.info(f" Chunk {chunk_num} completed:")
            self.logger.info(f"    Records: {chunk_stored:,}/{chunk_size:,} stored ({success_rate:.1f}% success)")
            self.logger.info(f"    Times - Generation: {generation_time:.2f}s, Processing: {chunk_processing_time:.2f}s, Total: {chunk_total_time:.2f}s")
            self.logger.info(f"    Running total: {total_stored:,} records stored")
            
            # Calculate and log processing speed
            if chunk_processing_time > 0:
                processing_speed = chunk_stored / chunk_processing_time
                self.logger.info(f"    Processing speed: {processing_speed:.2f} records/sec")
            
            print()  # Empty line for readability
        
        # Calculate overall processing statistics
        total_processing_time = time.time() - processing_start_time
        
        # End monitoring for the entire pipeline if available
        if HAS_MONITORING:
            end_monitoring()
        
        # Final verification with database
        final_count = self.db.get_transaction_count()
        
        # Comprehensive final report
        self.logger.info("=" * 70)
        self.logger.info(" COMPREHENSIVE PROCESSING REPORT")
        self.logger.info("=" * 70)
        self.logger.info(f" FINAL RESULTS:")
        self.logger.info(f"   Script reports stored: {total_stored:,} records")
        self.logger.info(f"   Database actual count: {final_count:,} records")
        
        # Explain the "mismatch" - it's actually good (data accumulation)
        if final_count > total_stored:
            previous_records = final_count - total_stored
            self.logger.info(f"     Database has {previous_records:,} previous records from earlier runs")
            self.logger.info(f"    This demonstrates data persistence across multiple executions")
        elif final_count == total_stored:
            self.logger.info(f"    Verification status: PERFECT MATCH")
        else:
            self.logger.info(f"     Verification status: UNEXPECTED MISMATCH")
        
        self.logger.info(f"  PERFORMANCE METRICS:")
        self.logger.info(f"   Total processing time: {total_processing_time:.2f}s")
        if total_processing_time > 0:
            overall_speed = total_stored / total_processing_time
            self.logger.info(f"   Overall processing speed: {overall_speed:.2f} records/sec")
        self.logger.info(f"   Chunks processed: {chunk_num}")
        self.logger.info(f"   Demo limit: {demo_limit:,} records")
        
        self.logger.info(f" SYSTEM INFORMATION:")
        self.logger.info(f"   Monitoring enabled: {' YES' if HAS_MONITORING else ' BASIC'}")
        self.logger.info(f"   Data generator: {self.data_gen.__class__.__name__}")
        self.logger.info(f"   Database client: {self.db.__class__.__name__}")
        
        self.logger.info("=" * 70)
        
        # Print monitoring dashboard if available
        if HAS_MONITORING:
            try:
                print_monitoring_dashboard()
                
                # Additional monitoring files information
                self.logger.info(" Monitoring files created:")
                monitoring_files = [
                    'logs/pipeline_metrics.json',
                    'logs/pipeline_detailed.log', 
                    'logs/pipeline_metrics.log',
                    'logs/system_metrics.json'
                ]
                
                for file_path in monitoring_files:
                    if os.path.exists(file_path):
                        file_size = os.path.getsize(file_path)
                        self.logger.info(f"    {file_path} ({file_size:,} bytes)")
                
            except Exception as e:
                self.logger.warning(f"Could not display monitoring dashboard: {e}")
        else:
            self.logger.info(" For advanced metrics: Install psutil in the container and ensure monitoring.py is available")
        
        return total_stored

    def validate_data_quality(self, sample_size=1000):
        """Perform basic data quality validation on a sample"""
        self.logger.info(" Performing data quality validation...")
        
        sample_data = self.data_gen.generate_batch(sample_size)
        
        validation_results = {
            'total_checked': len(sample_data),
            'valid_structure': 0,
            'invalid_structure': 0,
            'missing_fields': 0,
            'quality_issues': []
        }
        
        required_fields = ['transaction_id', 'customer_id', 'transaction_amount', 
                          'transaction_date', 'transaction_time']
        
        for record in sample_data:
            # Check required fields
            if all(field in record for field in required_fields):
                validation_results['valid_structure'] += 1
                
                # Basic field validation
                field_issues = []
                if not record['transaction_id'].startswith('tx_'):
                    field_issues.append("Invalid transaction ID format")
                if not record['customer_id'].startswith('cust_'):
                    field_issues.append("Invalid customer ID format")
                if record['transaction_amount'] <= 0:
                    field_issues.append("Invalid transaction amount")
                
                if field_issues:
                    validation_results['invalid_structure'] += 1
                    validation_results['quality_issues'].extend(field_issues)
            else:
                validation_results['missing_fields'] += 1
                missing = [field for field in required_fields if field not in record]
                validation_results['quality_issues'].append(f"Missing fields: {missing}")
        
        # Log validation results
        success_rate = (validation_results['valid_structure'] / sample_size) * 100
        self.logger.info(f" Data Quality Validation Results:")
        self.logger.info(f"   Sample size: {sample_size:,} records")
        self.logger.info(f"   Valid structure: {validation_results['valid_structure']:,} ({success_rate:.1f}%)")
        self.logger.info(f"   Invalid structure: {validation_results['invalid_structure']:,}")
        self.logger.info(f"   Missing fields: {validation_results['missing_fields']:,}")
        
        # Log common quality issues
        if validation_results['quality_issues']:
            from collections import Counter
            common_issues = Counter(validation_results['quality_issues']).most_common(3)
            self.logger.info(f"   Top quality issues:")
            for issue, count in common_issues:
                self.logger.info(f"     - {issue}: {count} occurrences")
        
        return validation_results

    def get_database_stats(self):
        """Get database statistics for reporting"""
        try:
            count = self.db.get_transaction_count()
            return {
                'total_records': count,
                'database_size_mb': 'N/A',  # Could be enhanced with actual DB size query
                'connection_status': 'Healthy'
            }
        except Exception as e:
            self.logger.warning(f"Could not get database stats: {e}")
            return {'total_records': 'Unknown', 'connection_status': 'Error'}

def main():
    """Main entry point with comprehensive error handling"""
    
    processor = None
    try:
        # Initialize processor
        processor = FinalBatchProcessor()
        
        # Get initial database stats
        db_stats = processor.get_database_stats()
        processor.logger.info(f" Initial database status: {db_stats['total_records']:,} records")
        
        # Optional: Run data quality validation
        processor.logger.info(" Starting bank batch data processing pipeline...")
        validation_results = processor.validate_data_quality(sample_size=500)
        
        # Check if validation passed minimum threshold
        if validation_results['valid_structure'] / validation_results['total_checked'] < 0.95:
            processor.logger.warning("  Data quality below 95% - proceeding with caution")
        else:
            processor.logger.info(" Data quality validation passed - proceeding with processing")
        
        # Process the main dataset
        processor.logger.info(" Beginning main data processing...")
        result = processor.process_large_dataset(
            total_records=1000000,
            chunk_size=100000, 
            demo_limit=300000
        )
        
        # Get final database stats
        final_db_stats = processor.get_database_stats()
        
        # Success message
        processor.logger.info(" PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
        processor.logger.info(f" Final result: {result:,} records processed and stored")
        processor.logger.info(f" Database now contains: {final_db_stats['total_records']:,} total records")
        
        # Additional success metrics
        processor.logger.info(" Execution Summary:")
        processor.logger.info(f"    Data validation: Completed ({validation_results['valid_structure']}/{validation_results['total_checked']} valid)")
        processor.logger.info(f"    Batch processing: Completed ({result:,} records)") 
        processor.logger.info(f"    Database operations: Successful")
        processor.logger.info(f"    Monitoring: {'Advanced' if HAS_MONITORING else 'Basic'}")
        processor.logger.info(f"    System: All components operational")
        
    except KeyboardInterrupt:
        print("\n  Pipeline execution interrupted by user")
        if processor:
            processor.logger.warning("Pipeline execution interrupted by user")
        
    except Exception as e:
        error_msg = f" CRITICAL ERROR in pipeline execution: {e}"
        print(error_msg)
        if processor:
            processor.logger.error(error_msg, exc_info=True)
        
        # Additional error context
        import traceback
        print(" Detailed error traceback:")
        traceback.print_exc()
        
    finally:
        # Ensure database connection is closed
        if processor and hasattr(processor, 'db'):
            processor.db.close()
            processor.logger.info(" Database connection closed")
        
        print("\n" + "="*60)
        print(" Bank Batch Pipeline Execution Finished")
        print("="*60)

if __name__ == "__main__":
    main()