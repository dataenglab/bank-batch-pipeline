# src/error_handler.py
import logging
import time
import json
from datetime import datetime
from typing import Callable, Any

class ErrorHandler:
    def __init__(self, log_file='pipeline_errors.log'):
        self.error_count = 0
        self.error_types = {}
        self.retry_attempts = {}
        self.setup_logging(log_file)
    
    def setup_logging(self, log_file):
        """Setup comprehensive logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def handle_error(self, error_type: str, error_message: str, context: dict = None, operation: Callable = None):
        """Handle different types of errors with appropriate strategies"""
        self.error_count += 1
        self.error_types[error_type] = self.error_types.get(error_type, 0) + 1
        
        error_info = {
            'timestamp': datetime.now().isoformat(),
            'error_type': error_type,
            'message': error_message,
            'context': context or {},
            'error_count': self.error_count
        }
        
        # Log the error
        self.logger.error(f"{error_type}: {error_message}")
        
        # Different strategies for different error types
        if error_type in ['network_error', 'timeout_error', 'database_connection', 'processing_error']:
            # Retry for transient errors
            if operation:
                return self.retry_operation(operation, error_type, max_retries=3)
            else:
                # If no operation provided, just log and continue
                self.logger.warning(f"No operation provided for {error_type}, continuing...")
                return True
        elif error_type in ['data_validation_error', 'business_rule_violation']:
            # Log and continue for data errors
            self.logger.warning(f"Data error - continuing processing: {error_message}")
            return None
        elif error_type in ['file_not_found', 'configuration_error']:
            # Critical errors - should stop processing
            self.logger.critical(f"Critical error - cannot continue: {error_message}")
            raise RuntimeError(f"Critical error: {error_message}")
        
        # Default: log and continue
        self.logger.warning(f"Unhandled error type {error_type}, continuing...")
        return True
    
    def retry_operation(self, operation: Callable, error_type: str, max_retries: int = 3):
        """Retry failed operations with exponential backoff"""
        for attempt in range(max_retries):
            try:
                wait_time = 2 ** attempt  # Exponential backoff: 1, 2, 4 seconds
                self.logger.info(f"Retry {attempt + 1}/{max_retries} for {error_type} after {wait_time}s")
                time.sleep(wait_time)
                
                result = operation()
                self.logger.info(f"Retry {attempt + 1} successful")
                return result  # Return the actual result!
                
            except Exception as e:
                self.logger.warning(f"Retry {attempt + 1} failed: {str(e)}")
                self.retry_attempts[error_type] = self.retry_attempts.get(error_type, 0) + 1
        
        self.logger.error(f"Operation failed after {max_retries} retries")
        return None  # Return None only if all retries fail
    
    def get_error_report(self):
        """Generate comprehensive error analysis report"""
        total_retries = sum(self.retry_attempts.values())
        
        return {
            'summary': {
                'total_errors': self.error_count,
                'unique_error_types': len(self.error_types),
                'total_retry_attempts': total_retries
            },
            'error_breakdown': self.error_types,
            'retry_breakdown': self.retry_attempts,
            'most_common_error': max(self.error_types, key=self.error_types.get) if self.error_types else None,
            'success_rate': self.calculate_success_rate()
        }
    
    def calculate_success_rate(self):
        """Calculate overall success rate based on retries"""
        if not self.retry_attempts:
            return 1.0
        
        successful_retries = sum(1 for attempts in self.retry_attempts.values() if attempts > 0)
        total_operations = len(self.retry_attempts)
        
        return successful_retries / total_operations if total_operations > 0 else 1.0
    
    def export_error_report(self, filename='error_analysis.json'):
        """Export comprehensive error analysis to JSON"""
        report = self.get_error_report()
        
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2)
        
        self.logger.info(f"Error analysis exported to {filename}")
        return report