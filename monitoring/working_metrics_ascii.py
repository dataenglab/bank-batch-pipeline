from prometheus_client import start_http_server, Counter, Gauge, Histogram
import time
import random

def main():
    print("=== Starting Pipeline Metrics Server ===")
    
    # Create metrics
    RECORDS_PROCESSED = Counter('pipeline_records_processed_total', 'Total records processed')
    PROCESSING_DURATION = Histogram('pipeline_processing_duration_seconds', 'Time spent processing records')
    ACTIVE_PROCESSES = Gauge('pipeline_active_processes', 'Number of active processing tasks')
    ERROR_COUNT = Counter('pipeline_errors_total', 'Total number of errors')
    QUEUE_SIZE = Gauge('pipeline_queue_size', 'Size of input queue')
    THROUGHPUT = Gauge('pipeline_throughput_records_per_second', 'Current processing throughput')

    # Start the server
    start_http_server(8000)
    print("SUCCESS: Prometheus metrics server started on port 8000")
    print("ACCESS:  Metrics available at: http://localhost:8000/metrics")
    print("STATUS:  Server is running...")
    print("COMMAND: Press Ctrl+C to stop the server")
    
    # Metrics simulation
    processed_count = 0
    last_update = time.time()
    
    try:
        while True:
            # Simulate processing
            ACTIVE_PROCESSES.set(1)
            start_time = time.time()
            
            processing_time = 0.1 + (0.4 * random.random())
            time.sleep(processing_time)
            
            # Record metrics
            RECORDS_PROCESSED.inc()
            PROCESSING_DURATION.observe(time.time() - start_time)
            processed_count += 1
            
            # Update other metrics
            QUEUE_SIZE.set(random.randint(10, 100))
            
            # Calculate throughput
            current_time = time.time()
            time_diff = current_time - last_update
            if time_diff >= 1.0:
                THROUGHPUT.set(processed_count / time_diff)
                processed_count = 0
                last_update = current_time
            
            # Simulate occasional errors
            if random.random() < 0.1:
                ERROR_COUNT.inc()
                
            ACTIVE_PROCESSES.set(0)
            
    except KeyboardInterrupt:
        print("Server stopped by user")

if __name__ == '__main__':
    main()