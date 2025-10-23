"""
Client and server using classes
"""

import logging
import socket

import const_cs
from context import lab_logging

from tel import tel


lab_logging.setup(stream_level=logging.INFO)  # init loging channels for the lab

# pylint: disable=logging-not-lazy, line-too-long

class Server:
    """ The server """
    _logger = logging.getLogger("vs2lab.lab1.clientserver.Server")
    _serving = True

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # prevents errors due to "addresses in use"
        self.sock.bind((const_cs.HOST, const_cs.PORT))
        self.sock.settimeout(3)  # time out in order not to block forever
        self._logger.info("Server bound to socket " + str(self.sock))

    def get_tel(self, name:str) -> str|None:
        self._logger.info('getting number for: ' + name)
        return tel.get(name)
    
    def format_get_result(self, number: str|None) -> str:
        self._logger.info('formatting result')
        command:str = ''
        msg:str = '\n'
        if not number:
            command = 'NOT_FOUND'
            msg = ''
        else:
            command = 'FOUND'
            msg = msg + number

        return command + msg

    def getall_tel(self) -> list[tuple[str, str]]:
        return list(tel.items())
    
    def format_getall_result(self, numbers: list[tuple[str, str]]) -> str:
        self._logger.info('formatting result')
        command:str = 'ENTRIES'

        numbers_str = [f"{name}: {number}" for name, number in numbers]
        msg = '\n' + ';'.join(numbers_str)

        return command + msg
        


    def serve(self):
        print(tel.items())

        """ Serve echo """
        self.sock.listen(1)

        while self._serving:  # as long as _serving (checked after connections or socket timeouts)
            try:
                # pylint: disable=unused-variable
                (connection, address) = self.sock.accept()  # returns new socket and address of client
                while True:  # forever
                    data = connection.recv(1024)  # receive data from client
                    if not data:
                        break
                    self._logger.info("Message received: " + repr(data.decode('ascii')))
                    data = data.decode('ascii')
                    data_lines = data.splitlines()
                    command = data_lines[0]
                    self._logger.info("Command: " +  command)
                    if command == 'GET':
                        query_param:str = data_lines[1]
                        tel_result = self.get_tel(query_param)
                        formatted_msg = self.format_get_result(tel_result)
                        self._logger.info("Sending message")
                        connection.send(formatted_msg.encode('ascii'))
                    elif command == 'GETALL':
                        tel_result = self.getall_tel()
                        formatted_msg = self.format_getall_result(tel_result)
                        self._logger.info("Sending message")
                        connection.send(formatted_msg.encode('ascii'))
                    else:
                        self._logger.warning("Command " + command + " not supported")
                        connection.send('ERR\nCommand not supported'.encode('ascii'))
                connection.close()  # close the connection
            except socket.timeout:
                pass  # ignore timeouts
        self.sock.close()
        self._logger.info("Server down.")

    
    


class Client:
    """ The client """
    logger = logging.getLogger("vs2lab.a1_layers.clientserver.Client")

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((const_cs.HOST, const_cs.PORT))
        self.logger.info("Client connected to socket " + str(self.sock))

    def call(self, msg_in:str="GE\njacka"):
        """ Call server """
        self.sock.send(msg_in.encode('ascii'))  # send encoded string as data
        data = self.sock.recv(1024)
        self.logger.info("Message received: " + repr(data.decode('ascii')))
        data = data.decode('ascii')
        data_lines = data.splitlines()

        command = data_lines[0]
        self.logger.info("Command: " +  command)
        msg:str = ''
        if command == 'FOUND':
            msg = data_lines[1]
        elif command == 'NOT_FOUND':
            msg = 'Person not in dictionary'
        elif command == 'ENTRIES':
            msg = '\n'.join(data_lines[1].split(';'))
        elif command == 'ERR':
            msg = data_lines[1]
        else:
            msg = 'Weird response from server'
            self.logger.warning("Command " + command + " not supported")
            

        print(msg)
        self.sock.close()  # close the connection
        self.logger.info("Client down.")
        return msg
    
    def send_get(self, name:str):
        self.call('GET\n'+name)

    def send_getall(self):
        self.call('GETALL')

    def close(self):
        """ Close socket """
        self.sock.close()
