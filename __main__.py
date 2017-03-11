#!/bin/env python
# -*- coding: utf8 -*-

import SocketServer
import logstash
import logging
logging.basicConfig()
from pyicap import *

#ICAP Port
port = 1344

# AMQP parameters
host = 'rabbitmq'
username = 'rabbitmq'
password= 'rabbitmq'
exchange = 'logstash'

logger = logging.getLogger('python-logstash-logger')
logger.setLevel(logging.INFO)
logger.addHandler(logstash.AMQPLogstashHandler(version=1,
                                                    host=host,
                                                    username=username,
                                                    durable=True,
                                                    password=password,
                                                    exchange=exchange,
                                                    exchange_routing_key='logstash'))

class ThreadingSimpleServer(SocketServer.ThreadingMixIn, ICAPServer):
    pass

class ICAPHandler(BaseICAPRequestHandler):

    def log_requests_OPTIONS(self):
        self.set_icap_response(200)
        self.set_icap_header('Methods', 'REQMOD')
        self.set_icap_header('Service', 'Pufferfish Logger')
        self.set_icap_header('Max-Connections', '100')
        self.set_icap_header('Options-TTL', '120')
        self.send_headers(False)
        logger.info("OPTIONS")

    def log_requests_REQMOD(self):
        self.set_icap_response(200)

        self.set_enc_request(' '.join(self.enc_req))
        for h in self.enc_req_headers:
            for v in self.enc_req_headers[h]:
                self.set_enc_header(h, v)

        # Read Body of Request
        if self.has_body:
            self.send_headers(True)
            request_body = ''
            while True:
                chunk = self.read_chunk()
                self.write_chunk(chunk)
                if chunk == '':
                    break
                request_body += chunk

            dict = request_dict(self.enc_req,self.enc_req_headers,request_body)
            logger.info('REQMOD',extra=dict)
            return

        # logging of Requests without Body
        if not self.has_body:
            self.send_headers(False)
            dict = request_dict(self.enc_req,self.enc_req_headers,'')
            logger.info('REQMOD',extra=dict)
            return


    def log_responses_OPTIONS(self):
        self.set_icap_response(200)
        self.set_icap_header('Methods', 'RESPMOD')
        self.set_icap_header('Service', 'Pufferfish Logger')
        self.set_icap_header('Transfer-Preview', '*')
        self.set_icap_header('Transfer-Ignore', 'jpg,jpeg,gif,png,swf,flv')
        self.set_icap_header('Transfer-Complete', '')
        self.set_icap_header('Max-Connections', '100')
        self.set_icap_header('Options-TTL', '120')
        self.send_headers(False)
        logger.info("OPTIONS")

    def log_responses_RESPMOD(self):
        self.set_icap_response(200)

        self.set_enc_status(' '.join(self.enc_res_status))
        for h in self.enc_res_headers:
            for v in self.enc_res_headers[h]:
                self.set_enc_header(h, v)

        if self.has_body:
            response_body = ''
            self.send_headers(True)
            while True:
                chunk = self.read_chunk()
                self.write_chunk(chunk)
                if chunk == '':
                    break
                response_body += chunk
            dict = {'status' : self.enc_res_status, 'headers' : self.enc_res_headers, 'body' : unicode(response_body, errors='ignore'),}
            logger.info('RESPMOD', extra=dict)
            return
        if not self.has_body:
            self.send_headers(False)
            dict = response_dict(self.enc_res_status,self.enc_res_headers,'')
            logger.info('RESPMOD', extra=dict)
            return




def request_dict(enc_req,enc_req_headers,body):
    return {
        'method': enc_req[0],
        'uri': enc_req[1],
        'protocol_version': enc_req[2],
        'headers': enc_req_headers,
        'body' : body,}

def response_dict(enc_res_status,enc_res_headers,body):
    return {'status' : enc_res_status,'headers': enc_res_headers,'body' : body,}

def main():
    server = ThreadingSimpleServer(('', port), ICAPHandler)
    while(True):
        server.handle_request()


if __name__ == "__main__":
    main()
