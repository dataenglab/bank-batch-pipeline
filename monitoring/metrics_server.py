from prometheus_client import start_http_server, Counter, Gauge, Histogram, generate_latest
import time
import threading
import random
from flask import Flask, Response

app = Flask(__name__)

# Create metrics
RECORDS_PROCESSED = Counter(
    'pipeline_records_processed_total', 
    'Total records processed',
    ['source', 'status']
)
PROCESSING_DURATION = Histogram(
    'pipeline_processing_duration_seconds', 
    'Time spent processing records',
    ['operation']
)
ACTIVE_PROCESSES = Gauge(
    'pipeline_active_processes', 
    'Number of active processing tasks'
)
ERROR_COUNT = Counter(
    'pipeline_errors_total', 
    'Total number of errors',
    ['error_type']
)
QUEUE_SIZE = Gauge(
    'pipeline_queue_size', 
    'Approximate size of input queue',
    ['queue_name']
)
MEMORY_USAGE = Gauge(
    'pipeline_memory_usage_bytes',
    'Memory usage of the pipeline'
)
THROUGHPUT = Gauge(
    'pipeline_throughput_records_per_second',
    'Current processing throughput'
)

# Initialize queue sizes
QUEUE_SIZE.labels(queue_name='input').set(0)
QUEUE_SIZE.labels(queue_name='processing').set(0)
QUEUE_SIZE.labels(queue_name='output').set(0)

class PipelineMetrics:
    def __init__(self):
        self.running = True
        self.processed_count = 0
        self.last_update = time.time()
        
    def simulate_pipeline_operations(self):
        """Simulate realistic pipeline metrics"""
        operations = ['ingestion', 'transformation', 'validation', 'aggregation']
        sources = ['kafka', 'api', 'file_upload']
        statuses = ['success', 'failed']
        error_types = ['validation', 'timeout', 'connection', 'processing']
        
        while self.running:
            try:
                # Simulate different pipeline operations
                operation = random.choice(operations)
                source = random.choice(sources)
                status = random.choice(['success', 'success', 'success', 'failed'])  # 75% success rate
                
                # Start processing
                ACTIVE_PROCESSES.inc()
                start_time = time.time()
                
                # Simulate processing time based on operation
                if operation == 'ingestion':
                    processing_time = 0.05 + (0.1 * random.random())
                elif operation == 'transformation':
                    processing_time = 0.1 + (0.3 * random.random())
                elif operation == 'validation':
                    processing_time = 0.02 + (0.05 * random.random())
                else:  # aggregation
                    processing_time = 0.2 + (0.5 * random.random())
                
                time.sleep(processing_time)
                
                # Record metrics based on status
                if status == 'success':
                    RECORDS_PROCESSED.labels(source=source, status='success').inc()
                    PROCESSING_DURATION.labels(operation=operation).observe(
                        time.time() - start_time
                    )
                    self.processed_count += 1
                else:
                    RECORDS_PROCESSED.labels(source=source, status='failed').inc()
                    error_type = random.choice(error_types)
                    ERROR_COUNT.labels(error_type=error_type).inc()
                
                # Update queue sizes realistically
                input_queue = max(0, random.randint(50, 200))
                processing_queue = max(0, random.randint(5, 50))
                output_queue = max(0, random.randint(10, 100))
                
                QUEUE_SIZE.labels(queue_name='input').set(input_queue)
                QUEUE_SIZE.labels(queue_name='processing').set(processing_queue)
                QUEUE_SIZE.labels(queue_name='output').set(output_queue)
                
                # Update memory usage
                memory_usage = 100 * 1024 * 1024 + (50 * 1024 * 1024 * random.random())  # 100-150 MB
                MEMORY_USAGE.set(memory_usage)
                
                # Calculate throughput
                current_time = time.time()
                time_diff = current_time - self.last_update
                if time_diff >= 1.0:  # Update throughput every second
                    throughput = self.processed_count / time_diff
                    THROUGHPUT.set(throughput)
                    self.processed_count = 0
                    self.last_update = current_time
                
                ACTIVE_PROCESSES.dec()
                
            except Exception as e:
                print(f"Error in metrics simulation: {e}")
                ERROR_COUNT.labels(error_type='simulation').inc()
                time.sleep(1)

@app.route('/metrics')
def metrics():
    """Expose Prometheus metrics"""
    return Response(generate_latest(), mimetype='text/plain')

@app.route('/health')
def health():
    """Health check endpoint"""
    return {'status': 'healthy', 'timestamp': time.time()}

@app.route('/')
def home():
    """Simple homepage with links"""
    return '''
    <h1>Data Pipeline Metrics</h1>
    <ul>
        <li><a href="/metrics">Prometheus Metrics</a></li>
        <li><a href="/health">Health Check</a></li>
    </ul>
    '''

def run_flask_app():
    """Run Flask app in a separate thread"""
    app.run(host='0.0.0.0', port=8000, debug=False, use_reloader=False)

def main():
    print("=== Starting Data Pipeline Metrics Server ===")
    
    # Start Prometheus metrics server on port 8001
    start_http_server(8001)
    print("Prometheus metrics: http://localhost:8001/metrics")
    
    # Start pipeline metrics simulation
    pipeline = PipelineMetrics()
    simulation_thread = threading.Thread(target=pipeline.simulate_pipeline_operations)
    simulation_thread.daemon = True
    simulation_thread.start()
    print("Pipeline metrics simulation started...")
    
    # Start Flask app on port 8000
    print("Web interface: http://localhost:8000")
    print("Health check: http://localhost:8000/health")
    print("Press Ctrl+C to stop the server")
    
    run_flask_app()

if __name__ == '__main__':
    main()