from prometheus_client import start_http_server, Counter, Gauge, Histogram
import time
import random

print("=== Starting Simple Metrics Server ===")

# Create metrics
RECORDS_PROCESSED = Counter('pipeline_records_processed_total', 'Total records processed')
PROCESSING_DURATION = Histogram('pipeline_processing_duration_seconds', 'Time spent processing records')
ACTIVE_PROCESSES = Gauge('pipeline_active_processes', 'Number of active processing tasks')
ERROR_COUNT = Counter('pipeline_errors_total', 'Total number of errors')
QUEUE_SIZE = Gauge('pipeline_queue_size', 'Size of input queue')
THROUGHPUT = Gauge('pipeline_throughput_records_per_second', 'Current processing throughput')

def simulate_pipeline_metrics():
    """Simple metrics simulation"""
    processed_count = 0
    last_update = time.time()
    
    while True:
        try:
            # Start processing
            ACTIVE_PROCESSES.inc()
            start_time = time.time()
            
            # Simulate processing time
            processing_time = 0.1 + (0.4 * random.random())
            time.sleep(processing_time)
            
            # Record successful processing
            RECORDS_PROCESSED.inc()
            PROCESSING_DURATION.observe(time.time() - start_time)
            processed_count += 1
            
            # Update queue size
            queue_size = random.randint(10, 100)
            QUEUE_SIZE.set(queue_size)
            
            # Calculate throughput
            current_time = time.time()
            time_diff = current_time - last_update
            if time_diff >= 1.0:
                throughput = processed_count / time_diff
                THROUGHPUT.set(throughput)
                processed_count = 0
                last_update = current_time
            
            # Occasionally simulate errors
            if random.random() < 0.1:  # 10% error rate
                ERROR_COUNT.inc()
            
            ACTIVE_PROCESSES.dec()
            
        except Exception as e:
            print(f"Error in metrics simulation: {e}")
            time.sleep(1)

if __name__ == '__main__':
    # Start Prometheus metrics server on port 8000
    start_http_server(8000)
    print("Prometheus metrics available at: http://localhost:8000/metrics")
    print("Press Ctrl+C to stop the server")
    
    # Start metrics simulation
    simulate_pipeline_metrics()