from measurements import Measurements
from Collecting_threads.collecting_threads import *
import threading



collect_new_flow_thread.start()
collect_rtt_thread.start()
collect_throughput_thread.start()
collect_queue_delay_thread.start()
collect_retr_thread.start()

