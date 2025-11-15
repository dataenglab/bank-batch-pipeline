"""
Comprehensive Monitoring and Metrics for Batch Pipeline
Real-time performance tracking, resource monitoring, and metrics collection
"""

import time
import logging
import json
import os
import psutil
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any
from enum import Enum


class MetricType(Enum):
    """Types of metrics that can be tracked"""
    PROCESSING = "processing"
    MEMORY = "memory"
    CPU = "cpu"
    DATABASE = "database"
    VALIDATION = "validation"
    ERROR = "error"


@dataclass
class BatchMetrics:
    """Comprehensive metrics for a single batch processing run"""
    
    # Identification
    batch_id: str
    pipeline_name: str = "bank_batch_pipeline"
    
    # Timing
    start_time: datetime = None
    end_time: Optional[datetime] = None
    processing_time: float = 0.0
    
    # Record counts
    records_processed: int = 0
    records_successful: int = 0
    records_failed: int = 0
    records_skipped: int = 0
    
    # Performance metrics
    memory_usage_mb: float = 0.0
    cpu_percent: float = 0.0
    disk_io_read_mb: float = 0.0
    disk_io_write_mb: float = 0.0
    network_io_sent_mb: float = 0.0
    network_io_received_mb: float = 0.0
    
    # Quality metrics
    data_quality_score: float = 0.0
    validation_errors: int = 0
    database_errors: int = 0
    
    # Custom tags for filtering
    tags: List[str] = None
    
    def __post_init__(self):
        if self.start_time is None:
            self.start_time = datetime.now()
        if self.tags is None:
            self.tags = []
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage"""
        if self.records_processed == 0:
            return 0.0
        return (self.records_successful / self.records_processed) * 100
    
    @property
    def records_per_second(self) -> float:
        """Calculate processing speed in records per second"""
        if self.processing_time == 0:
            return 0.0
        return self.records_processed / self.processing_time
    
    @property
    def error_rate(self) -> float:
        """Calculate error rate as percentage"""
        if self.records_processed == 0:
            return 0.0
        return ((self.records_failed + self.validation_errors + self.database_errors) / 
                self.records_processed) * 100
    
    @property
    def duration_seconds(self) -> float:
        """Calculate total duration in seconds"""
        if self.end_time and self.start_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary for JSON serialization"""
        data = asdict(self)
        # Convert datetime objects to ISO format strings
        if isinstance(data['start_time'], datetime):
            data['start_time'] = data['start_time'].isoformat()
        if isinstance(data['end_time'], datetime):
            data['end_time'] = data['end_time'].isoformat()
        
        # Add computed properties
        data.update({
            'success_rate': self.success_rate,
            'records_per_second': self.records_per_second,
            'error_rate': self.error_rate,
            'duration_seconds': self.duration_seconds
        })
        
        return data
    
    def add_tag(self, tag: str):
        """Add a tag to the batch"""
        if tag not in self.tags:
            self.tags.append(tag)


@dataclass
class SystemMetrics:
    """System-level resource metrics"""
    timestamp: datetime
    cpu_percent: float
    memory_percent: float
    memory_used_mb: float
    memory_available_mb: float
    disk_usage_percent: float
    disk_used_gb: float
    network_bytes_sent: int
    network_bytes_received: int
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        return data


class AlertManager:
    """Manages alerts and notifications based on metrics"""
    
    def __init__(self, alert_thresholds: Dict[str, float] = None):
        self.alert_thresholds = alert_thresholds or {
            'success_rate_below': 95.0,
            'error_rate_above': 5.0,
            'memory_usage_above': 80.0,
            'cpu_usage_above': 85.0,
            'processing_speed_below': 100.0  # records per second
        }
        self.alerts_triggered = []
        
    def check_batch_alerts(self, metrics: BatchMetrics) -> List[str]:
        """Check if batch metrics trigger any alerts"""
        alerts = []
        
        if metrics.success_rate < self.alert_thresholds['success_rate_below']:
            alerts.append(f"Low success rate: {metrics.success_rate:.1f}%")
            
        if metrics.error_rate > self.alert_thresholds['error_rate_above']:
            alerts.append(f"High error rate: {metrics.error_rate:.1f}%")
            
        if metrics.memory_usage_mb > self.alert_thresholds['memory_usage_above']:
            alerts.append(f"High memory usage: {metrics.memory_usage_mb:.1f}MB")
            
        if metrics.cpu_percent > self.alert_thresholds['cpu_usage_above']:
            alerts.append(f"High CPU usage: {metrics.cpu_percent:.1f}%")
            
        if (metrics.records_per_second < self.alert_thresholds['processing_speed_below'] and 
            metrics.records_processed > 1000):
            alerts.append(f"Slow processing speed: {metrics.records_per_second:.1f} records/sec")
        
        # Record triggered alerts
        for alert in alerts:
            self.alerts_triggered.append({
                'timestamp': datetime.now().isoformat(),
                'batch_id': metrics.batch_id,
                'alert': alert
            })
            
        return alerts


class PipelineMonitor:
    """Main monitoring class for the batch pipeline with comprehensive tracking"""
    
    def __init__(self, pipeline_name: str = "bank_batch_pipeline"):
        self.pipeline_name = pipeline_name
        self.metrics_history: List[BatchMetrics] = []
        self.system_metrics_history: List[SystemMetrics] = []
        self.current_batch: Optional[BatchMetrics] = None
        self.alert_manager = AlertManager()
        
        # Performance tracking
        self.performance_stats = {
            'total_batches': 0,
            'total_records_processed': 0,
            'total_processing_time': 0.0,
            'peak_memory_usage': 0.0,
            'peak_cpu_usage': 0.0
        }
        
        self.setup_logging()
        self.setup_directories()
        
    def setup_logging(self):
        """Setup comprehensive logging with multiple handlers"""
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        
        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
        )
        simple_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # Configure root logger
        self.logger = logging.getLogger("PipelineMonitor")
        self.logger.setLevel(logging.INFO)
        
        # Clear any existing handlers
        self.logger.handlers.clear()
        
        # File handler for detailed logs
        file_handler = logging.FileHandler(f"{log_dir}/pipeline_detailed.log", encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(detailed_formatter)
        
        # File handler for metrics
        metrics_handler = logging.FileHandler(f"{log_dir}/pipeline_metrics.log", encoding='utf-8')
        metrics_handler.setLevel(logging.INFO)
        metrics_handler.setFormatter(simple_formatter)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(simple_formatter)
        
        # Add all handlers
        self.logger.addHandler(file_handler)
        self.logger.addHandler(metrics_handler)
        self.logger.addHandler(console_handler)
        
        self.logger.info(" Pipeline monitoring initialized")
        
    def setup_directories(self):
        """Ensure all required directories exist"""
        directories = ['logs', 'metrics', 'reports']
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
    
    def start_batch(self, batch_id: str, tags: List[str] = None):
        """Start monitoring a new batch with comprehensive tracking"""
        self.current_batch = BatchMetrics(
            batch_id=batch_id,
            pipeline_name=self.pipeline_name,
            tags=tags or []
        )
        
        # Capture initial system state
        self._capture_system_metrics()
        
        self.logger.info(f" Starting batch monitoring: {batch_id}")
        self.logger.info(f"   Tags: {tags or ['default']}")
        
    def end_batch(self):
        """End monitoring for current batch and perform comprehensive analysis"""
        if not self.current_batch:
            self.logger.warning("No active batch to end")
            return
            
        try:
            # Set end time and calculate duration
            self.current_batch.end_time = datetime.now()
            self.current_batch.processing_time = self.current_batch.duration_seconds
            
            # Capture final system metrics
            self._capture_system_metrics()
            self._capture_final_batch_metrics()
            
            # Check for alerts
            alerts = self.alert_manager.check_batch_alerts(self.current_batch)
            
            # Add to history and update performance stats
            self.metrics_history.append(self.current_batch)
            self._update_performance_stats()
            
            # Log batch completion with comprehensive details
            self._log_batch_completion(alerts)
            
            # Save metrics and generate reports
            self.save_metrics()
            self.generate_batch_report()
            
        except Exception as e:
            self.logger.error(f"Error ending batch monitoring: {e}")
        finally:
            self.current_batch = None
    
    def record_processing(self, records_attempted: int, records_successful: int, 
                         records_skipped: int = 0, validation_errors: int = 0, 
                         database_errors: int = 0):
        """Record processing results with detailed error tracking"""
        if self.current_batch:
            self.current_batch.records_processed += records_attempted
            self.current_batch.records_successful += records_successful
            self.current_batch.records_failed += (records_attempted - records_successful)
            self.current_batch.records_skipped += records_skipped
            self.current_batch.validation_errors += validation_errors
            self.current_batch.database_errors += database_errors
            
            # Update data quality score (simplified calculation)
            total_errors = (self.current_batch.records_failed + 
                          self.current_batch.validation_errors + 
                          self.current_batch.database_errors)
            total_processed = self.current_batch.records_processed
            
            if total_processed > 0:
                self.current_batch.data_quality_score = (
                    (total_processed - total_errors) / total_processed * 100
                )
    
    def record_custom_metric(self, metric_type: MetricType, value: float, 
                           description: str = ""):
        """Record custom metrics for specialized tracking"""
        if self.current_batch:
            # Store custom metrics as additional attributes
            if not hasattr(self.current_batch, 'custom_metrics'):
                self.current_batch.custom_metrics = {}
                
            metric_key = f"{metric_type.value}_{description}" if description else metric_type.value
            self.current_batch.custom_metrics[metric_key] = {
                'value': value,
                'timestamp': datetime.now().isoformat(),
                'type': metric_type.value,
                'description': description
            }
            
            self.logger.debug(f"Custom metric recorded: {metric_key} = {value}")
    
    def _capture_system_metrics(self):
        """Capture comprehensive system resource usage"""
        try:
            process = psutil.Process()
            system = psutil
            
            # Process-specific metrics
            memory_info = process.memory_info()
            cpu_percent = process.cpu_percent()
            
            # System-wide metrics
            virtual_memory = system.virtual_memory()
            disk_usage = system.disk_usage('/')
            net_io = system.net_io_counters()
            
            system_metrics = SystemMetrics(
                timestamp=datetime.now(),
                cpu_percent=cpu_percent,
                memory_percent=virtual_memory.percent,
                memory_used_mb=virtual_memory.used / 1024 / 1024,
                memory_available_mb=virtual_memory.available / 1024 / 1024,
                disk_usage_percent=disk_usage.percent,
                disk_used_gb=disk_usage.used / 1024 / 1024 / 1024,
                network_bytes_sent=net_io.bytes_sent if net_io else 0,
                network_bytes_received=net_io.bytes_recv if net_io else 0
            )
            
            self.system_metrics_history.append(system_metrics)
            
            # Update current batch metrics if active
            if self.current_batch:
                self.current_batch.memory_usage_mb = memory_info.rss / 1024 / 1024
                self.current_batch.cpu_percent = cpu_percent
                
                # Update disk and network I/O (cumulative)
                io_counters = process.io_counters()
                if io_counters:
                    self.current_batch.disk_io_read_mb = io_counters.read_bytes / 1024 / 1024
                    self.current_batch.disk_io_write_mb = io_counters.write_bytes / 1024 / 1024
                
        except Exception as e:
            self.logger.warning(f"Could not capture system metrics: {e}")
    
    def _capture_final_batch_metrics(self):
        """Capture final metrics before batch completion"""
        if self.current_batch:
            # One final system metrics capture
            self._capture_system_metrics()
            
            # Ensure processing time is calculated
            if self.current_batch.end_time and self.current_batch.start_time:
                self.current_batch.processing_time = (
                    self.current_batch.end_time - self.current_batch.start_time
                ).total_seconds()
    
    def _update_performance_stats(self):
        """Update overall performance statistics"""
        if not self.current_batch:
            return
            
        self.performance_stats['total_batches'] += 1
        self.performance_stats['total_records_processed'] += self.current_batch.records_processed
        self.performance_stats['total_processing_time'] += self.current_batch.processing_time
        
        # Track peaks
        if self.current_batch.memory_usage_mb > self.performance_stats['peak_memory_usage']:
            self.performance_stats['peak_memory_usage'] = self.current_batch.memory_usage_mb
            
        if self.current_batch.cpu_percent > self.performance_stats['peak_cpu_usage']:
            self.performance_stats['peak_cpu_usage'] = self.current_batch.cpu_percent
    
    def _log_batch_completion(self, alerts: List[str]):
        """Log comprehensive batch completion information"""
        batch = self.current_batch
        
        self.logger.info(" BATCH PROCESSING COMPLETED")
        self.logger.info(f"   Batch ID: {batch.batch_id}")
        self.logger.info(f"   Duration: {batch.processing_time:.2f}s")
        self.logger.info(f"   Records: {batch.records_processed:,} processed, "
                        f"{batch.records_successful:,} successful, "
                        f"{batch.records_failed:,} failed")
        self.logger.info(f"   Success Rate: {batch.success_rate:.1f}%")
        self.logger.info(f"   Processing Speed: {batch.records_per_second:.2f} records/sec")
        self.logger.info(f"   Data Quality Score: {batch.data_quality_score:.1f}%")
        self.logger.info(f"   Memory Usage: {batch.memory_usage_mb:.2f} MB")
        self.logger.info(f"   CPU Usage: {batch.cpu_percent:.1f}%")
        
        if alerts:
            self.logger.warning("     ALERTS TRIGGERED:")
            for alert in alerts:
                self.logger.warning(f"     - {alert}")
    
    def save_metrics(self):
        """Save comprehensive metrics to JSON files"""
        try:
            # Save batch metrics
            batch_metrics_data = {
                'pipeline_name': self.pipeline_name,
                'total_batches': len(self.metrics_history),
                'performance_stats': self.performance_stats,
                'batches': [metric.to_dict() for metric in self.metrics_history],
                'overall_stats': self.get_overall_stats(),
                'alerts_triggered': self.alert_manager.alerts_triggered,
                'last_updated': datetime.now().isoformat()
            }
            
            with open('logs/pipeline_metrics.json', 'w', encoding='utf-8') as f:
                json.dump(batch_metrics_data, f, indent=2, ensure_ascii=False)
            
            # Save system metrics (last 100 to avoid huge files)
            recent_system_metrics = self.system_metrics_history[-100:]
            system_metrics_data = {
                'system_metrics': [metric.to_dict() for metric in recent_system_metrics],
                'last_updated': datetime.now().isoformat()
            }
            
            with open('logs/system_metrics.json', 'w', encoding='utf-8') as f:
                json.dump(system_metrics_data, f, indent=2, ensure_ascii=False)
                
            self.logger.debug("Metrics saved successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to save metrics: {e}")
    
    def generate_batch_report(self):
        """Generate a detailed report for the current batch"""
        if not self.current_batch:
            return
            
        try:
            report = {
                'batch_report': {
                    'batch_id': self.current_batch.batch_id,
                    'summary': self.current_batch.to_dict(),
                    'performance_analysis': self._analyze_batch_performance(),
                    'recommendations': self._generate_recommendations(),
                    'generated_at': datetime.now().isoformat()
                }
            }
            
            report_file = f"reports/batch_{self.current_batch.batch_id}_report.json"
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
                
            self.logger.info(f"Batch report generated: {report_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to generate batch report: {e}")
    
    def _analyze_batch_performance(self) -> Dict[str, Any]:
        """Analyze batch performance and identify potential issues"""
        if not self.current_batch:
            return {}
            
        batch = self.current_batch
        analysis = {
            'performance_rating': 'EXCELLENT',
            'bottlenecks': [],
            'strengths': []
        }
        
        # Performance rating
        if batch.success_rate >= 99:
            analysis['performance_rating'] = 'EXCELLENT'
        elif batch.success_rate >= 95:
            analysis['performance_rating'] = 'GOOD'
        elif batch.success_rate >= 90:
            analysis['performance_rating'] = 'FAIR'
        else:
            analysis['performance_rating'] = 'POOR'
        
        # Identify bottlenecks
        if batch.records_per_second < 100:
            analysis['bottlenecks'].append("Low processing speed")
        if batch.memory_usage_mb > 500:
            analysis['bottlenecks'].append("High memory usage")
        if batch.cpu_percent > 80:
            analysis['bottlenecks'].append("High CPU usage")
        if batch.error_rate > 5:
            analysis['bottlenecks'].append("High error rate")
        
        # Identify strengths
        if batch.success_rate > 99:
            analysis['strengths'].append("Excellent success rate")
        if batch.records_per_second > 1000:
            analysis['strengths'].append("High processing speed")
        if batch.memory_usage_mb < 100:
            analysis['strengths'].append("Efficient memory usage")
        
        return analysis
    
    def _generate_recommendations(self) -> List[str]:
        """Generate recommendations for performance improvement"""
        if not self.current_batch:
            return []
            
        recommendations = []
        batch = self.current_batch
        
        if batch.success_rate < 95:
            recommendations.append("Investigate root causes of processing failures")
        if batch.records_per_second < 100:
            recommendations.append("Consider optimizing database queries or increasing batch size")
        if batch.memory_usage_mb > 500:
            recommendations.append("Monitor memory usage and consider memory optimization")
        if batch.error_rate > 5:
            recommendations.append("Review data validation rules and error handling")
        
        return recommendations
    
    def get_overall_stats(self) -> Dict[str, Any]:
        """Calculate comprehensive overall pipeline statistics"""
        if not self.metrics_history:
            return {}
            
        total_records = sum(b.records_processed for b in self.metrics_history)
        total_successful = sum(b.records_successful for b in self.metrics_history)
        total_time = sum(b.processing_time for b in self.metrics_history)
        total_batches = len(self.metrics_history)
        
        # Calculate averages
        avg_success_rate = (total_successful / total_records * 100) if total_records > 0 else 0
        avg_processing_speed = total_records / total_time if total_time > 0 else 0
        
        # Get recent performance (last 10 batches)
        recent_batches = self.metrics_history[-10:] if len(self.metrics_history) >= 10 else self.metrics_history
        recent_success_rate = sum(b.success_rate for b in recent_batches) / len(recent_batches)
        
        return {
            'total_batches_processed': total_batches,
            'total_records_processed': total_records,
            'total_successful_records': total_successful,
            'total_failed_records': total_records - total_successful,
            'overall_success_rate': avg_success_rate,
            'recent_success_rate': recent_success_rate,
            'average_processing_speed': avg_processing_speed,
            'total_processing_time': total_time,
            'average_memory_usage_mb': sum(b.memory_usage_mb for b in self.metrics_history) / total_batches,
            'average_cpu_percent': sum(b.cpu_percent for b in self.metrics_history) / total_batches,
            'performance_trend': 'improving' if len(recent_batches) > 1 and 
                                recent_batches[-1].success_rate > recent_batches[0].success_rate else 'stable'
        }
    
    def print_dashboard(self):
        """Print a comprehensive monitoring dashboard"""
        stats = self.get_overall_stats()
        
        print("\n" + "="*70)
        print(" COMPREHENSIVE PIPELINE MONITORING DASHBOARD")
        print("="*70)
        
        if stats:
            print(f"\n OVERALL STATISTICS:")
            print(f"   Total Batches Processed: {stats['total_batches_processed']:,}")
            print(f"   Total Records: {stats['total_records_processed']:,}")
            print(f"   Successful Records: {stats['total_successful_records']:,}")
            print(f"   Failed Records: {stats['total_failed_records']:,}")
            print(f"   Overall Success Rate: {stats['overall_success_rate']:.1f}%")
            print(f"   Recent Success Rate: {stats['recent_success_rate']:.1f}%")
            print(f"   Average Speed: {stats['average_processing_speed']:.2f} records/sec")
            print(f"   Total Time: {stats['total_processing_time']:.2f}s")
            
            print(f"\n RESOURCE USAGE:")
            print(f"   Avg Memory: {stats['average_memory_usage_mb']:.2f} MB")
            print(f"   Avg CPU: {stats['average_cpu_percent']:.1f}%")
            print(f"   Peak Memory: {self.performance_stats['peak_memory_usage']:.2f} MB")
            print(f"   Peak CPU: {self.performance_stats['peak_cpu_usage']:.1f}%")
            
            print(f"\n PERFORMANCE TRENDS:")
            print(f"   Trend: {stats['performance_trend'].upper()}")
            
            if self.alert_manager.alerts_triggered:
                print(f"\n  RECENT ALERTS ({len(self.alert_manager.alerts_triggered)}):")
                for alert in self.alert_manager.alerts_triggered[-5:]:  # Last 5 alerts
                    print(f"   - {alert['alert']} (Batch: {alert['batch_id']})")
        else:
            print("No metrics data available yet. Process some batches to see statistics.")
            
        print("="*70)
    
    def get_recent_metrics(self, hours: int = 24) -> List[BatchMetrics]:
        """Get metrics from the last specified hours"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        return [m for m in self.metrics_history 
                if m.start_time and m.start_time >= cutoff_time]
    
    def clear_old_metrics(self, days_to_keep: int = 30):
        """Clear metrics older than specified days to manage storage"""
        cutoff_time = datetime.now() - timedelta(days=days_to_keep)
        self.metrics_history = [m for m in self.metrics_history 
                              if m.start_time and m.start_time >= cutoff_time]
        self.logger.info(f"Cleared metrics older than {days_to_keep} days")


# Global monitor instance for easy access
monitor = PipelineMonitor()

# Convenience functions for quick monitoring
def start_monitoring(batch_id: str, tags: List[str] = None):
    """Quick start monitoring for a batch"""
    monitor.start_batch(batch_id, tags)

def end_monitoring():
    """Quick end monitoring for current batch"""
    monitor.end_batch()

def record_processing_results(records_attempted: int, records_successful: int, 
                            records_skipped: int = 0, validation_errors: int = 0, 
                            database_errors: int = 0):
    """Quick record processing results"""
    monitor.record_processing(records_attempted, records_successful, 
                            records_skipped, validation_errors, database_errors)

def print_monitoring_dashboard():
    """Quick print monitoring dashboard"""
    monitor.print_dashboard()

if __name__ == "__main__":
    # Test the monitoring system
    print("Testing Pipeline Monitoring System...")
    
    # Simulate a batch
    monitor.start_batch("test_batch_001", tags=["test", "development"])
    
    # Simulate some processing
    monitor.record_processing(1000, 950, 10, 30, 10)
    monitor.record_custom_metric(MetricType.VALIDATION, 95.5, "validation_success_rate")
    
    # End the batch
    monitor.end_batch()
    
    # Print dashboard
    monitor.print_dashboard()
    
    print("Monitoring test completed successfully!")