import json
import threading

def collect_rtt_measurements(self):
    
    while True:
        # Receive measurements from the P4 switch
        measurements = self.Socket.recv(2048)
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
            print("\nAn error occurred in the rtt thread:", str(e),"\n")
            exit()  

def start_rtt_measurements_thread(self):
    collect_rtt_measurements_thread = threading.Thread(target=collect_rtt_measurements(self), name="collect_rtt_measurements")
    collect_rtt_measurements_thread.start()

