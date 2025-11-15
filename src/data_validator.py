# src/data_validator.py
import pandas as pd
import logging
from datetime import datetime
import json

class DataValidator:
    def __init__(self):
        self.validation_errors = []
        self.logger = logging.getLogger(__name__)
        
    def validate_transaction(self, transaction):
        """Validate individual transaction record"""
        checks = {
            'amount_positive': transaction.get('amount', 0) > 0,
            'valid_timestamp': self._is_valid_timestamp(transaction.get('timestamp')),
            'account_not_empty': bool(transaction.get('account_id')),
            'reasonable_amount': transaction.get('amount', 0) <= 1000000,  # $1M limit
            'valid_currency': transaction.get('currency') in ['USD', 'EUR', 'GBP']
        }
        
        if not all(checks.values()):
            failed_checks = [k for k, v in checks.items() if not v]
            error_info = {
                'record': transaction,
                'failed_checks': failed_checks,
                'timestamp': datetime.now().isoformat()
            }
            self.validation_errors.append(error_info)
            self.logger.warning(f"Validation failed: {failed_checks}")
            return False
        return True
    
    def _is_valid_timestamp(self, timestamp):
        """Validate timestamp is reasonable"""
        try:
            if not timestamp:
                return False
            ts = pd.to_datetime(timestamp)
            return ts.year >= 2020 and ts <= datetime.now()
        except:
            return False
    
    def validate_batch(self, transactions):
        """Validate a batch of transactions"""
        valid_count = 0
        invalid_count = 0
        
        for transaction in transactions:
            if self.validate_transaction(transaction):
                valid_count += 1
            else:
                invalid_count += 1
        
        return {
            'valid_count': valid_count,
            'invalid_count': invalid_count,
            'total_processed': len(transactions),
            'validity_rate': valid_count / len(transactions) if transactions else 0
        }
    
    def get_validation_report(self):
        """Generate comprehensive validation summary"""
        total_checked = len(self.validation_errors) + sum(1 for _ in self.validation_errors)
        
        # Analyze error patterns
        error_patterns = {}
        for error in self.validation_errors:
            for check in error['failed_checks']:
                error_patterns[check] = error_patterns.get(check, 0) + 1
        
        return {
            'summary': {
                'total_checked': total_checked,
                'errors_count': len(self.validation_errors),
                'error_rate': len(self.validation_errors) / total_checked if total_checked > 0 else 0,
                'success_rate': 1 - (len(self.validation_errors) / total_checked) if total_checked > 0 else 1
            },
            'error_breakdown': error_patterns,
            'sample_errors': self.validation_errors[:5] if self.validation_errors else []  # Fixed this line
        }
    
    def export_errors(self, filename):
        """Export validation errors to JSON file"""
        report = self.get_validation_report()
        
        with open(filename, 'w') as f:
            json.dump({
                'validation_report': report,
                'all_errors': self.validation_errors
            }, f, indent=2)
        
        self.logger.info(f"Validation errors exported to {filename}")