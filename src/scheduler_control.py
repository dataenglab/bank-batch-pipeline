"""
Batch Scheduler Control Utility
Command-line interface to manage the batch processing scheduler
"""

import subprocess
import sys
import os
import time
import json
from datetime import datetime

class SchedulerController:
    """Controller for managing the batch processing scheduler"""
    
    def __init__(self):
        self.scheduler_process = None
        self.log_file = "logs/scheduler.log"
        self.history_file = "logs/job_history.json"
        self.pid_file = "logs/scheduler.pid"
        
    def ensure_logs_directory(self):
        """Ensure logs directory exists"""
        os.makedirs("logs", exist_ok=True)
    
    def print_banner(self):
        """Print application banner"""
        print("=" * 60)
        print("BATCH PROCESSING SCHEDULER CONTROL PANEL")
        print("=" * 60)
    
    def save_pid(self, pid):
        """Save scheduler PID to file"""
        try:
            with open(self.pid_file, 'w') as f:
                f.write(str(pid))
        except Exception as e:
            print(f"Warning: Could not save PID: {e}")
    
    def get_saved_pid(self):
        """Get saved scheduler PID"""
        try:
            if os.path.exists(self.pid_file):
                with open(self.pid_file, 'r') as f:
                    return int(f.read().strip())
        except Exception:
            pass
        return None
    
    def is_scheduler_running_simple(self):
        """Simple check if scheduler is running using PID file"""
        pid = self.get_saved_pid()
        if not pid:
            return False
        
        try:
            # Check if process exists (Unix/Linux/Mac method)
            if os.name == 'posix':
                os.kill(pid, 0)
                return True
            else:
                # Windows - try tasklist
                result = subprocess.run(
                    ['tasklist', '/fi', f'pid eq {pid}'], 
                    capture_output=True, 
                    text=True,
                    encoding='utf-8',
                    errors='ignore'
                )
                return str(pid) in result.stdout
        except (OSError, subprocess.SubprocessError):
            return False
    
    def start_scheduler(self, background=True):
        """Start the scheduler"""
        self.ensure_logs_directory()
        
        print("Starting Batch Processing Scheduler...")
        
        # Check if scheduler is already running
        if self.is_scheduler_running_simple():
            print("Warning: Scheduler is already running!")
            self.show_scheduler_status()
            return False
        
        try:
            if background:
                # Start in background with proper encoding
                self.scheduler_process = subprocess.Popen(
                    [sys.executable, "src/scheduler.py"],
                    stdout=open(self.log_file, 'a', encoding='utf-8'),
                    stderr=subprocess.STDOUT
                )
                self.save_pid(self.scheduler_process.pid)
                print("Scheduler started in background")
                print(f"Process ID: {self.scheduler_process.pid}")
            else:
                # Start in foreground
                print("Starting scheduler in foreground (Ctrl+C to stop)...")
                subprocess.run([sys.executable, "src/scheduler.py"])
                return True
                
            # Wait a moment and check if it's running
            time.sleep(2)
            if self.is_scheduler_running_simple():
                print("Scheduler started successfully!")
                self.show_recent_logs(5)
                return True
            else:
                print("Scheduler failed to start")
                return False
                
        except Exception as e:
            print(f"Error starting scheduler: {e}")
            return False
    
    def stop_scheduler(self):
        """Stop the scheduler"""
        print("Stopping Batch Processing Scheduler...")
        
        if not self.is_scheduler_running_simple():
            print("Scheduler is not running")
            # Clean up PID file if it exists
            if os.path.exists(self.pid_file):
                os.remove(self.pid_file)
            return True
        
        try:
            pid = self.get_saved_pid()
            if pid:
                if os.name == 'posix':
                    # Unix/Linux/Mac
                    subprocess.run(['kill', str(pid)])
                else:
                    # Windows
                    subprocess.run(['taskkill', '/pid', str(pid), '/f'])
                
                time.sleep(2)
                
                # Check if still running
                if not self.is_scheduler_running_simple():
                    print("Scheduler stopped successfully")
                    if os.path.exists(self.pid_file):
                        os.remove(self.pid_file)
                    return True
                else:
                    print("Failed to stop scheduler")
                    return False
            else:
                print("No scheduler PID found")
                return True
                
        except Exception as e:
            print(f"Error stopping scheduler: {e}")
            # Clean up PID file on error
            if os.path.exists(self.pid_file):
                os.remove(self.pid_file)
            return False
    
    def restart_scheduler(self):
        """Restart the scheduler"""
        print("Restarting Batch Processing Scheduler...")
        
        if self.is_scheduler_running_simple():
            self.stop_scheduler()
            time.sleep(3)
        
        return self.start_scheduler()
    
    def show_scheduler_status(self):
        """Show current scheduler status"""
        print("\nSCHEDULER STATUS")
        print("-" * 40)
        
        if self.is_scheduler_running_simple():
            print("Status: RUNNING")
            pid = self.get_saved_pid()
            if pid:
                print(f"PID: {pid}")
        else:
            print("Status: STOPPED")
        
        # Show job history if available
        if os.path.exists(self.history_file):
            try:
                with open(self.history_file, 'r', encoding='utf-8') as f:
                    history = json.load(f)
                    stats = history.get('scheduler_stats', {})
                    
                    print(f"\nSTATISTICS:")
                    print(f"  Jobs Executed: {stats.get('jobs_executed', 0)}")
                    print(f"  Jobs Succeeded: {stats.get('jobs_succeeded', 0)}")
                    print(f"  Jobs Failed: {stats.get('jobs_failed', 0)}")
                    
                    if stats.get('jobs_executed', 0) > 0:
                        success_rate = (stats.get('jobs_succeeded', 0) / stats.get('jobs_executed', 0)) * 100
                        print(f"  Success Rate: {success_rate:.1f}%")
                    
                    if stats.get('last_execution'):
                        last_exec = datetime.fromisoformat(stats['last_execution'])
                        print(f"  Last Execution: {last_exec.strftime('%Y-%m-%d %H:%M:%S')}")
                        
            except Exception as e:
                print(f"  Could not load history: {e}")
    
    def show_recent_logs(self, lines=10):
        """Show recent log entries"""
        if not os.path.exists(self.log_file):
            print("No log file found")
            return
        
        try:
            print(f"\nRECENT LOGS (last {lines} lines):")
            print("-" * 40)
            
            with open(self.log_file, 'r', encoding='utf-8', errors='ignore') as f:
                log_lines = f.readlines()
                for line in log_lines[-lines:]:
                    print(f"  {line.strip()}")
                    
        except Exception as e:
            print(f"Error reading log file: {e}")
    
    def safe_string_check(self, text, keywords):
        """Safely check if text contains any keywords"""
        if not text:
            return False
        return any(keyword in text for keyword in keywords)
    
    def run_batch_job(self):
        """Run a single batch job manually"""
        print("Running Batch Job Manually...")
        print("-" * 40)
        
        try:
            # Use the working processor file with proper encoding
            result = subprocess.run([
                "docker-compose", "run", "--rm", 
                "batch-processor", 
                "python", "src/final_working_processor_fixed.py"
            ], capture_output=True, text=True, encoding='utf-8', errors='ignore')
            
            if result.returncode == 0:
                print("Batch job completed successfully!")
                
                # Safely check and display output
                stdout = result.stdout or ""
                
                if "FINAL RESULTS:" in stdout:
                    print("\nKEY RESULTS:")
                    for line in stdout.split('\n'):
                        if self.safe_string_check(line, ['FINAL RESULTS', 'records stored', 'COMPLETED', 'DEBUG: Actual database count']):
                            print(f"  {line.strip()}")
                
                # Also show processing summary
                if "Processing Chunk" in stdout:
                    print("\nPROCESSING SUMMARY:")
                    for line in stdout.split('\n'):
                        if self.safe_string_check(line, ['Processing Chunk', 'Stored:', 'Success rate:', 'Total:']):
                            print(f"  {line.strip()}")
                            
                # Show final completion message
                if "COMPLETED!" in stdout:
                    for line in stdout.split('\n'):
                        if "COMPLETED!" in line or "Final:" in line:
                            print(f"  {line.strip()}")
                            
            else:
                print("Batch job failed!")
                print(f"  Return code: {result.returncode}")
                stderr = result.stderr or ""
                stdout = result.stdout or ""
                
                if stderr:
                    # Show only relevant error parts
                    error_lines = stderr.split('\n')
                    for line in error_lines:
                        if line.strip() and not line.startswith('Container'):
                            print(f"  Error: {line.strip()}")
                else:
                    # Show last part of output for debugging
                    print(f"  Output (last 300 chars): {stdout[-300:]}")
                
            return result.returncode == 0
            
        except Exception as e:
            print(f"Error running batch job: {e}")
            import traceback
            print(f"Detailed error: {traceback.format_exc()}")
            return False
    
    def show_help(self):
        """Show help information"""
        print("\nUSAGE GUIDE:")
        print("-" * 40)
        print("python scheduler_control.py [COMMAND]")
        print("\nAvailable Commands:")
        print("  start     - Start scheduler in background")
        print("  stop      - Stop scheduler")
        print("  restart   - Restart scheduler")
        print("  status    - Show scheduler status")
        print("  run       - Run batch job once")
        print("  logs      - Show recent logs")
        print("  help      - Show this help message")
        print("\nExamples:")
        print("  python scheduler_control.py start")
        print("  python scheduler_control.py status")
        print("  python scheduler_control.py run")
    
    def interactive_mode(self):
        """Run in interactive mode"""
        self.print_banner()
        
        while True:
            print("\nChoose an option:")
            print("1. Start Scheduler")
            print("2. Stop Scheduler") 
            print("3. Restart Scheduler")
            print("4. Show Status")
            print("5. Run Batch Job")
            print("6. Show Logs")
            print("7. Help")
            print("0. Exit")
            
            try:
                choice = input("\nEnter your choice (0-7): ").strip()
                
                if choice == '1':
                    self.start_scheduler()
                elif choice == '2':
                    self.stop_scheduler()
                elif choice == '3':
                    self.restart_scheduler()
                elif choice == '4':
                    self.show_scheduler_status()
                elif choice == '5':
                    self.run_batch_job()
                elif choice == '6':
                    self.show_recent_logs()
                elif choice == '7':
                    self.show_help()
                elif choice == '0':
                    print("Goodbye!")
                    break
                else:
                    print("Invalid choice. Please try again.")
                    
            except KeyboardInterrupt:
                print("\nGoodbye!")
                break
            except Exception as e:
                print(f"Error: {e}")

def main():
    """Main entry point"""
    # Set encoding for Windows compatibility
    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='ignore')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='ignore')
    
    controller = SchedulerController()
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "start":
            controller.start_scheduler()
        elif command == "stop":
            controller.stop_scheduler()
        elif command == "restart":
            controller.restart_scheduler()
        elif command == "status":
            controller.show_scheduler_status()
        elif command == "run":
            controller.run_batch_job()
        elif command == "logs":
            controller.show_recent_logs()
        elif command in ["help", "-h", "--help"]:
            controller.show_help()
        else:
            print(f"Unknown command: {command}")
            controller.show_help()
    else:
        # No arguments - start interactive mode
        controller.interactive_mode()

if __name__ == "__main__":
    main()