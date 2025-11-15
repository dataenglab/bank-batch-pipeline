"""
Batch Processing Scheduler
Automated scheduling for bank batch pipeline processing
"""

import schedule
import time
import subprocess
import logging
import os
import sys
from datetime import datetime, timedelta
import json

# Add src directory to path for imports
sys.path.append('/app/src')

try:
    from monitoring import metrics
    HAS_MONITORING = True
except ImportError:
    HAS_MONITORING = False
    print(" Monitoring module not available")

class BatchScheduler:
    """Advanced scheduler for batch processing pipeline"""
    
    def __init__(self):
        self.setup_logging()
        self.job_history = []
        self.scheduler_stats = {
            'jobs_executed': 0,
            'jobs_succeeded': 0,
            'jobs_failed': 0,
            'total_execution_time': 0,
            'last_execution': None
        }
        
    def setup_logging(self):
        """Configure comprehensive logging"""
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f"{log_dir}/scheduler.log"),
                logging.FileHandler(f"{log_dir}/scheduler_debug.log", level=logging.DEBUG),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger("BatchScheduler")
        self.logger.info(" Scheduler logging initialized")
        
    def run_batch_job(self, job_type="daily"):
        """Execute the batch processing job with enhanced monitoring"""
        job_start = datetime.now()
        job_id = f"job_{job_start.strftime('%Y%m%d_%H%M%S')}_{job_type}"
        
        self.logger.info(f" Starting batch job: {job_id}")
        
        job_record = {
            'job_id': job_id,
            'job_type': job_type,
            'start_time': job_start.isoformat(),
            'status': 'running',
            'output': '',
            'error': ''
        }
        
        try:
            # Run the batch processor
            self.logger.info(f"Executing: docker-compose run --rm batch-processor python src/enhanced_processor.py")
            
            result = subprocess.run([
                "docker-compose", "run", "--rm", 
                "batch-processor", 
                "python", "src/enhanced_processor.py"
            ], 
            capture_output=True, 
            text=True, 
            timeout=3600  # 1 hour timeout
            )
            
            job_duration = (datetime.now() - job_start).total_seconds()
            
            # Update job record
            job_record.update({
                'end_time': datetime.now().isoformat(),
                'duration_seconds': job_duration,
                'return_code': result.returncode,
                'output': result.stdout[-1000:],  # Last 1000 chars
                'error': result.stderr
            })
            
            if result.returncode == 0:
                job_record['status'] = 'success'
                self.scheduler_stats['jobs_succeeded'] += 1
                self.logger.info(f" Batch job {job_id} completed successfully in {job_duration:.2f}s")
                
                # Log key performance indicators
                if "FINAL RESULTS:" in result.stdout:
                    self.logger.info(" Extracting performance metrics from output...")
                    # You could parse specific metrics here
                    
            else:
                job_record['status'] = 'failed'
                self.scheduler_stats['jobs_failed'] += 1
                self.logger.error(f" Batch job {job_id} failed with return code {result.returncode}")
                self.logger.error(f"Error output: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            job_duration = (datetime.now() - job_start).total_seconds()
            job_record.update({
                'end_time': datetime.now().isoformat(),
                'duration_seconds': job_duration,
                'status': 'timeout',
                'error': f'Job exceeded timeout limit of 3600 seconds'
            })
            self.scheduler_stats['jobs_failed'] += 1
            self.logger.error(f" Batch job {job_id} timed out after {job_duration:.2f}s")
            
        except Exception as e:
            job_duration = (datetime.now() - job_start).total_seconds()
            job_record.update({
                'end_time': datetime.now().isoformat(),
                'duration_seconds': job_duration,
                'status': 'error',
                'error': str(e)
            })
            self.scheduler_stats['jobs_failed'] += 1
            self.logger.error(f" Batch job {job_id} encountered error: {e}")
        
        # Update scheduler statistics
        self.scheduler_stats['jobs_executed'] += 1
        self.scheduler_stats['total_execution_time'] += job_duration
        self.scheduler_stats['last_execution'] = datetime.now().isoformat()
        
        # Store job history
        self.job_history.append(job_record)
        
        # Keep only last 100 jobs in memory
        if len(self.job_history) > 100:
            self.job_history = self.job_history[-100:]
            
        # Save job history to file
        self.save_job_history()
        
        return job_record
    
    def run_daily_maintenance(self):
        """Daily batch processing job"""
        return self.run_batch_job("daily_maintenance")
    
    def run_weekly_reporting(self):
        """Weekly comprehensive processing"""
        return self.run_batch_job("weekly_reporting")
    
    def run_test_job(self):
        """Test job for development and monitoring"""
        return self.run_batch_job("test")
    
    def save_job_history(self):
        """Save job history to JSON file for persistence"""
        try:
            history_data = {
                'scheduler_stats': self.scheduler_stats,
                'job_history': self.job_history,
                'last_updated': datetime.now().isoformat()
            }
            
            with open('logs/job_history.json', 'w') as f:
                json.dump(history_data, f, indent=2)
                
        except Exception as e:
            self.logger.error(f"Failed to save job history: {e}")
    
    def load_job_history(self):
        """Load job history from JSON file"""
        try:
            if os.path.exists('logs/job_history.json'):
                with open('logs/job_history.json', 'r') as f:
                    history_data = json.load(f)
                    self.scheduler_stats = history_data.get('scheduler_stats', self.scheduler_stats)
                    self.job_history = history_data.get('job_history', [])
                self.logger.info(" Loaded previous job history")
        except Exception as e:
            self.logger.warning(f"Could not load job history: {e}")
    
    def print_scheduler_status(self):
        """Print current scheduler status and statistics"""
        self.logger.info(" SCHEDULER STATUS")
        self.logger.info(f"  Jobs Executed: {self.scheduler_stats['jobs_executed']}")
        self.logger.info(f"  Jobs Succeeded: {self.scheduler_stats['jobs_succeeded']}")
        self.logger.info(f"  Jobs Failed: {self.scheduler_stats['jobs_failed']}")
        
        if self.scheduler_stats['jobs_executed'] > 0:
            success_rate = (self.scheduler_stats['jobs_succeeded'] / self.scheduler_stats['jobs_executed']) * 100
            avg_time = self.scheduler_stats['total_execution_time'] / self.scheduler_stats['jobs_executed']
            self.logger.info(f"  Success Rate: {success_rate:.1f}%")
            self.logger.info(f"  Average Job Time: {avg_time:.2f}s")
        
        if self.scheduler_stats['last_execution']:
            last_exec = datetime.fromisoformat(self.scheduler_stats['last_execution'])
            self.logger.info(f"  Last Execution: {last_exec.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Show upcoming jobs
        self.logger.info(" UPCOMING SCHEDULED JOBS:")
        jobs = schedule.get_jobs()
        for job in jobs:
            self.logger.info(f"  - {job}")
    
    def setup_schedules(self):
        """Configure all scheduled jobs"""
        
        # Daily maintenance job at 2 AM
        schedule.every().day.at("02:00").do(self.run_daily_maintenance).tag("production", "maintenance")
        
        # Weekly comprehensive job on Sunday at 3 AM
        schedule.every().sunday.at("03:00").do(self.run_weekly_reporting).tag("production", "reporting")
        
        # Test job every 30 minutes (for development)
        schedule.every(30).minutes.do(self.run_test_job).tag("development", "test")
        
        # Health check every hour
        schedule.every().hour.do(self.health_check).tag("monitoring", "health")
        
        self.logger.info(" All schedules configured")
    
    def health_check(self):
        """Perform system health check"""
        self.logger.info("  Performing health check...")
        
        # Check if docker-compose is available
        try:
            result = subprocess.run(["docker-compose", "version"], capture_output=True, text=True)
            if result.returncode == 0:
                self.logger.info(" Docker Compose is available")
            else:
                self.logger.error(" Docker Compose not available")
        except Exception as e:
            self.logger.error(f" Docker Compose check failed: {e}")
        
        # Check disk space (simplified)
        try:
            disk_usage = os.statvfs('/')
            free_gb = (disk_usage.f_bavail * disk_usage.f_frsize) / (1024 ** 3)
            if free_gb > 1:  # More than 1GB free
                self.logger.info(f" Disk space: {free_gb:.1f}GB free")
            else:
                self.logger.warning(f"  Low disk space: {free_gb:.1f}GB free")
        except Exception as e:
            self.logger.error(f" Disk space check failed: {e}")
    
    def run(self):
        """Main scheduler loop"""
        self.logger.info(" BATCH PROCESSING SCHEDULER STARTING")
        self.logger.info("=" * 50)
        
        # Load previous history
        self.load_job_history()
        
        # Setup all schedules
        self.setup_schedules()
        
        # Print initial status
        self.print_scheduler_status()
        
        self.logger.info(" Entering main scheduler loop...")
        self.logger.info("Press Ctrl+C to stop the scheduler")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
                
                # Print status every hour
                if datetime.now().minute == 0:
                    self.print_scheduler_status()
                    
        except KeyboardInterrupt:
            self.logger.info(" Scheduler stopped by user")
        except Exception as e:
            self.logger.error(f" Scheduler crashed: {e}")
        finally:
            self.logger.info(" Final scheduler statistics:")
            self.print_scheduler_status()
            self.logger.info(" Scheduler shutdown complete")

def main():
    """Main entry point"""
    scheduler = BatchScheduler()
    scheduler.run()

if __name__ == "__main__":
    main()