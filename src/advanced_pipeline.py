# src/advanced_pipeline.py
import sys
import os
import time
import random
import pandas as pd
from datetime import datetime
# Import your advanced features
from data_validator import DataValidator
from error_handler import ErrorHandler
class AdvancedPipeline:
    def __init__(self):
        self.validator = DataValidator()
        self.error_handler = ErrorHandler('advanced_pipeline_errors.log')
        self.processed_count = 0
        self.start_time = time.time()
    def generate_sample_data(self, count=20):
        """Generate sample transaction data for testing with more valid records"""
        sample_data = []
        for i in range(count):
            # Create 80% valid, 20% invalid transactions
            if i % 5 == 0:  # 20% invalid
                # Generate different types of invalid data
                invalid_type = i % 4
                if invalid_type == 0:
                    # Empty account
                    transaction = {
                        'transaction_id': f'TX{i:04d}',
                        'account_id': '',
                        'amount': random.uniform(10, 1000),
                        'currency': 'USD',
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                elif invalid_type == 1:
                    # Negative amount
                    transaction = {
                        'transaction_id': f'TX{i:04d}',
                        'account_id': f'ACC{i:05d}',
                        'amount': -random.uniform(10, 1000),
                        'currency': 'EUR',
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                elif invalid_type == 2:
                    # Invalid currency
                    transaction = {
                        'transaction_id': f'TX{i:04d}',
                        'account_id': f'ACC{i:05d}',
                        'amount': random.uniform(10, 1000),
                        'currency': 'XYZ',
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                else:
                    # Invalid timestamp
                    transaction = {
                        'transaction_id': f'TX{i:04d}',
                        'account_id': f'ACC{i:05d}',
                        'amount': random.uniform(10, 1000),
                        'currency': 'GBP',
                        'timestamp': 'invalid_date'
                    }
            else:
                # Valid transaction (80%)
                transaction = {
                    'transaction_id': f'TX{i:04d}',
                    'account_id': f'ACC{i:05d}',
                    'amount': random.uniform(10, 1000),
                    'currency': random.choice(['USD', 'EUR', 'GBP']),
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            sample_data.append(transaction)
        return sample_data
    def _is_valid_timestamp(self, timestamp):
        """Validate timestamp is reasonable"""
        try:
            if not timestamp:
                return False
            ts = pd.to_datetime(timestamp)
            return ts.year >= 2020 and ts <= datetime.now()
        except:
            return False
    def validate_single_transaction(self, transaction):
        """Validate a single transaction and return detailed results"""
        checks = {
            'amount_positive': transaction.get('amount', 0) > 0,
            'valid_timestamp': self._is_valid_timestamp(transaction.get('timestamp')),
            'account_not_empty': bool(transaction.get('account_id')),
            'reasonable_amount': transaction.get('amount', 0) <= 1000000,
            'valid_currency': transaction.get('currency') in ['USD', 'EUR', 'GBP']
        }
        return all(checks.values()), checks
    def process_single_transaction(self, transaction):
        """Process a single transaction with error handling"""
        # First, validate the transaction
        is_valid, checks = self.validate_single_transaction(transaction)
        if not is_valid:
            failed_checks = [k for k, v in checks.items() if not v]
            self.error_handler.handle_error(
                'data_validation_error',
                f"Transaction {transaction['transaction_id']} failed validation",
                {
                    'transaction_id': transaction['transaction_id'],
                    'failed_checks': failed_checks,
                    'transaction_data': transaction
                }
            )
            return None
        # Define the processing operation
        def process_operation():
            # Simulate processing work
            processing_time = random.uniform(0.01, 0.1)
            time.sleep(processing_time)
            # Simulate occasional transient errors (10% chance)
            if random.random() < 0.1:
                error_types = [
                    ConnectionError("Database connection timeout"),
                    TimeoutError("API request timed out"),
                    RuntimeError("Temporary processing failure")
                ]
                raise random.choice(error_types)
            # Simulate successful processing
            processed_data = {
                **transaction,
                'processed_at': datetime.now().isoformat(),
                'processing_id': f'PROC{random.randint(1000, 9999)}',
                'status': 'completed'
            }
            return processed_data
        # Execute with error handling
        try:
            result = self.error_handler.handle_error(
                'processing_error',
                f"Failed to process transaction {transaction['transaction_id']}",
                {
                    'transaction_id': transaction['transaction_id'],
                    'operation': 'transaction_processing'
                },
                process_operation
            )
            if result:
                self.processed_count += 1
                return result
            else:
                return None
        except Exception as e:
            self.error_handler.handle_error(
                'unexpected_error',
                f"Unexpected error during processing: {str(e)}",
                {
                    'transaction_id': transaction['transaction_id'],
                    'error_details': str(e)
                }
            )
            return None
    def process_transactions_batch(self, transactions):
        """Process a batch of transactions with comprehensive error handling"""
        print("=== ADVANCED PIPELINE PROCESSING ===")
        print(f"Processing {len(transactions)} transactions...")
        successful_processing = []
        failed_processing = []
        for i, transaction in enumerate(transactions, 1):
            print(f"Processing transaction {i}/{len(transactions)}: {transaction['transaction_id']}")
            result = self.process_single_transaction(transaction)
            if result:
                successful_processing.append(result)
                print(f"  SUCCESS: Processed {transaction['transaction_id']}")
            else:
                failed_processing.append(transaction)
                print(f"  FAILED: Could not process {transaction['transaction_id']}")
        return successful_processing, failed_processing
    def simulate_database_operation(self):
        """Simulate a database operation that might fail"""
        def db_operation():
            # Simulate database work
            time.sleep(0.05)
            # 15% chance of database error
            if random.random() < 0.15:
                raise ConnectionError("Database connection lost")
            return "Database operation completed successfully"
        return self.error_handler.handle_error(
            'database_connection',
            "Database operation failed",
            {'operation': 'batch_commit'},
            db_operation
        )
    def generate_comprehensive_report(self, successful_transactions, failed_transactions):
        """Generate a comprehensive pipeline performance report"""
        processing_time = time.time() - self.start_time
        throughput = self.processed_count / processing_time if processing_time > 0 else 0
        validation_report = self.validator.get_validation_report()
        error_report = self.error_handler.get_error_report()
        # Calculate additional metrics
        total_transactions = len(successful_transactions) + len(failed_transactions)
        success_rate = len(successful_transactions) / total_transactions if total_transactions > 0 else 0
        report = {
            'pipeline_summary': {
                'total_transactions_processed': total_transactions,
                'successful_transactions': len(successful_transactions),
                'failed_transactions': len(failed_transactions),
                'success_rate': success_rate,
                'processing_time_seconds': round(processing_time, 2),
                'throughput_records_per_second': round(throughput, 2),
                'completion_time': datetime.now().isoformat(),
                'pipeline_version': '1.0'
            },
            'data_quality_metrics': {
                'validation_success_rate': validation_report['summary']['success_rate'],
                'total_validation_checks': validation_report['summary']['total_checked'],
                'validation_errors': validation_report['summary']['errors_count'],
                'common_validation_issues': validation_report['error_breakdown']
            },
            'error_handling_metrics': {
                'total_errors_handled': error_report['summary']['total_errors'],
                'unique_error_types': error_report['summary']['unique_error_types'],
                'total_retry_attempts': error_report['summary']['total_retry_attempts'],
                'error_handling_success_rate': error_report['success_rate'],
                'error_type_breakdown': error_report['error_breakdown']
            },
            'performance_metrics': {
                'average_processing_time_per_record': round(processing_time / total_transactions, 4) if total_transactions > 0 else 0,
                'records_processed_per_second': round(throughput, 2),
                'pipeline_efficiency': round(success_rate * 100, 2)
            }
        }
        return report
    def export_pipeline_report(self, report, filename='pipeline_performance_report.json'):
        """Export the comprehensive pipeline report to JSON"""
        import json
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        print(f"Pipeline performance report exported to {filename}")
        return filename
    def run_complete_demo(self, transaction_count=25):
        """Run a complete demonstration of all advanced features working together"""
        print("STARTING ADVANCED PIPELINE DEMO")
        print("=" * 60)
        # Step 1: Generate sample data
        print("\n1. GENERATING SAMPLE DATA")
        transactions = self.generate_sample_data(transaction_count)
        print(f"   Generated {len(transactions)} sample transactions")
        print(f"   Sample valid transaction: {transactions[1]}")
        print(f"   Sample invalid transaction: {transactions[0]}")
        # Step 2: Process transactions with advanced features
        print("\n2. PROCESSING TRANSACTIONS")
        print("   Using advanced data validation and error handling...")
        successful_transactions, failed_transactions = self.process_transactions_batch(transactions)
        # Step 3: Simulate final database operation
        print("\n3. FINAL DATABASE OPERATION")
        db_result = self.simulate_database_operation()
        if db_result:
            print(f"   Database operation: {db_result}")
        else:
            print("   Database operation failed after retries")
        # Step 4: Generate comprehensive reports
        print("\n4. GENERATING REPORTS")
        report = self.generate_comprehensive_report(successful_transactions, failed_transactions)
        # Export all reports
        self.validator.export_errors('demo_validation_errors.json')
        self.error_handler.export_error_report('demo_error_analysis.json')
        self.export_pipeline_report(report, 'pipeline_performance_report.json')
        # Step 5: Display summary
        print("\n5. DEMONSTRATION SUMMARY")
        print("=" * 60)
        # Pipeline Summary
        summary = report['pipeline_summary']
        print(f"\nPIPELINE PERFORMANCE:")
        print(f"  Total transactions: {summary['total_transactions_processed']}")
        print(f"  Successful: {summary['successful_transactions']}")
        print(f"  Failed: {summary['failed_transactions']}")
        print(f"  Success rate: {summary['success_rate']:.2%}")
        print(f"  Processing time: {summary['processing_time_seconds']}s")
        print(f"  Throughput: {summary['throughput_records_per_second']} records/sec")
        # Data Quality
        quality = report['data_quality_metrics']
        print(f"\nDATA QUALITY:")
        print(f"  Validation success rate: {quality['validation_success_rate']:.2%}")
        print(f"  Validation errors: {quality['validation_errors']}")
        # Error Handling
        errors = report['error_handling_metrics']
        print(f"\nERROR HANDLING:")
        print(f"  Total errors handled: {errors['total_errors_handled']}")
        print(f"  Unique error types: {errors['unique_error_types']}")
        print(f"  Retry attempts: {errors['total_retry_attempts']}")
        print(f"  Error handling success: {errors['error_handling_success_rate']:.2%}")
        # Performance
        performance = report['performance_metrics']
        print(f"\nPERFORMANCE METRICS:")
        print(f"  Avg processing time: {performance['average_processing_time_per_record']}s/record")
        print(f"  Pipeline efficiency: {performance['pipeline_efficiency']}%")
        # File exports
        print(f"\nEXPORTED REPORTS:")
        print(f"  - demo_validation_errors.json (Data validation details)")
        print(f"  - demo_error_analysis.json (Error handling analysis)")
        print(f"  - pipeline_performance_report.json (Comprehensive metrics)")
        print(f"  - advanced_pipeline_errors.log (Detailed error log)")
        print("\n" + "=" * 60)
        print("ADVANCED PIPELINE DEMONSTRATION COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        return report, successful_transactions, failed_transactions
def main():
    """Main function to run the advanced pipeline demonstration"""
    try:
        # Initialize the advanced pipeline
        pipeline = AdvancedPipeline()
        # Run the complete demonstration
        report, successful, failed = pipeline.run_complete_demo(30)
        # Display sample of successful processing
        if successful:
            print(f"\nSAMPLE SUCCESSFUL PROCESSING RESULT:")
            sample_result = successful[0]
            for key, value in sample_result.items():
                print(f"  {key}: {value}")
        return True
    except Exception as e:
        print(f"Pipeline demonstration failed: {str(e)}")
        return False
if __name__ == '__main__':
    success = main()
  # Replace the last few lines in advanced_pipeline.py:
    print("\nAdvanced pipeline demonstration completed successfully!")
else:
    print("\nAdvanced pipeline demonstration failed.")
print("\nThank you for using the Advanced Data Pipeline!")