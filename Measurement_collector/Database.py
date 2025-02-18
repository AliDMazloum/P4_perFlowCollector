import socket
import json
from datetime import datetime
import secrets
  

"""
    This file declares the database class. The class initiate the necessary connections with an InfluxDB database. 
"""

class database:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect()
    def connect(self):
        try:
            self.socket.connect((self.host, self.port))
            print("Connected to", self.host, "on port", self.port)
        except ConnectionRefusedError:
            print("Connection refused. Make sure the remote host and port are correct and accessible.")
        except Exception as e:
            print("An error occurred:", str(e))

    def write_measurement(self, message):
        # if (message['per_flow_measurement']['type'] == "long_flow"):
        #     message['flow_start_time'] =  datetime.utcfromtimestamp(message['flow_start_time']).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        #     message['flow_end_time'] =  datetime.utcfromtimestamp(message['flow_end_time']).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        #     print(message['flow_end_time'])
            
        if(message['per_flow_measurement']['type'] != "new_flow_report"):
            message[message["metric_name"]] = float(message[message["metric_name"]])     
            # print(message)
        json_data = json.dumps(message)
        json_data += "\n"
        try:
            self.socket.sendall(json_data.encode())
        except Exception as e:
            print("An error occurred while sending data:", str(e))
        
    def close(self):
        try:
            self.socket.close()
            print("Connection closed.")
        except Exception as e:
            print("An error occurred while closing the connection:", str(e))
