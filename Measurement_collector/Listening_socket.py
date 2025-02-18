import socket

"""
    This file declares the Socket class. The class initiate a connection with the control plane of the switch. The main objective is 
    transfer the data outside the control plane which only have python 2.6. By tranferring the data, the user will have access to python 3.9. 
"""

class Socket():
    def __init__(self,host=socket.gethostname(), port=60002):  
        sock = socket.socket()  # Create a socket object
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))  # Bind to the port
        # print('*' * 60)
        # print('Waiting for connection from the P4 switch...')
        sock.listen(10)  # Now wait for client connection.
        self.listener, addr = sock.accept()  # Establish connection with client.
        print('Connected to port ',port)
        # print('*' * 60)
        # print('\n')
    
    def get_Listener(self):
        return self.listener