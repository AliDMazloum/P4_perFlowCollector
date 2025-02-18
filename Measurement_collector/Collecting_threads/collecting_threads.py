import threading
import os
import sys

sys.path.append('../../Measurement_collector')
from measurements import Measurements


def collect_new_flow():
    measurements_new_flow = Measurements(60002)

def collect_rtt():
    measurements_rtt = Measurements(60003)

def collect_throughput():
    measurements_throughput = Measurements(60004)

def collect_queue_delay():
    measurements_queue_delay = Measurements(60005)

def collect_retr():
    measurements_retr = Measurements(60006)

collect_new_flow_thread = threading.Thread(target=collect_new_flow, name="collect_new_flow")
collect_rtt_thread = threading.Thread(target=collect_rtt, name="collect_rtt")
collect_throughput_thread = threading.Thread(target=collect_throughput, name="collect_throughput")
collect_queue_delay_thread = threading.Thread(target=collect_queue_delay, name="collect_queue_delay")
collect_retr_thread = threading.Thread(target=collect_retr, name="collect_retr")
