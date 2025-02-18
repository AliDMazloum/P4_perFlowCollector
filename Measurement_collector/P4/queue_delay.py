import json
import threading

def collect_queue_delay_measurements(self):
    
    while True:
        # Receive measurements from the P4 switch
        measurements = self.Socket.recv(1024)
        measurements = measurements.decode('utf-8')
        measurements = measurements.strip().split("\n")
        
        try:
            for report in measurements:
                report = json.loads(report)
                self.DB.write_measurement(report)

        except json.decoder.JSONDecodeError:
            pass
            if measurements == ['']:
                break
        except Exception as e:
            exception_name = type(e).__name__
            print("\nAn error occurred in the queue delay thread:", str(e),"\n")
            exit()  

def start_queue_delay_measurements_thread(self):
    collect_queue_delay_measurements_thread = threading.Thread(target=collect_queue_delay_measurements(self), name="collect_queue_delay_measurements")
    collect_queue_delay_measurements_thread.start()

