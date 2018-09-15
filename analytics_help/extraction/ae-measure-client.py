import requests
import sys
import json
import time
import logging.config
from bs4 import BeautifulSoup
import csv
import traceback
from logging.handlers import TimedRotatingFileHandler
import logging
import os
import logging.config
import socket


logger = logging.getLogger()
try:
    host_ip = socket.gethostbyname(socket.gethostname())
except:
    host_ip = 'unknown'

service_ip = "10.117.238.115"

def config_loggers():
    logging.config.fileConfig('{}/logging-client.conf'.format(os.path.dirname(os.path.abspath(__file__))))
    for handler in logging.root.handlers:
        if isinstance(handler, TimedRotatingFileHandler):
            logger.addHandler(handler)
            logger.info('Configured application logger to rotatingFileHandler.')
            logging.getLogger('backoff').addHandler(handler)
            break
    logging.getLogger('backoff').setLevel(logging.INFO)
    logger.info('finished configuring loggers')


if __name__ == '__main__':
    config_loggers()

    if len(sys.argv) <4:
        print ("Please enter valid arguments \nex:python ae-measure-client.py  batch_id  attribute filename")
        exit(1)

    batch_id = sys.argv[1]
    attribute = sys.argv[2]
    file_name = sys.argv[3]
    host = "http://{}".format(service_ip)
    start_time_main = time.time()
    if batch_id.strip() == "":
        print ("Invalid batch_id - \n Please enter batch_id,attribute,input_file_name   ")

    if attribute.strip() == "":
        print ("Invalid attribute - \n Please enter batch_id,attribute,input_file_name ")

    if file_name.strip() == "":
        print ("Invaid input file - \n Please enter batch_id,attribute,input_file_name ")

    requests_session = requests.Session()
    requests_session.mount('http://', requests.adapters.HTTPAdapter(pool_connections=5, pool_maxsize=3))
    response_body_sucess = []
    response_body_failure = []
    cnt_to_print_log = 0

    url = '{}/predictions'.format(host)
    headers = {'Content-Type': 'text/plain', 'X-Attr': attribute}
    try:
        with open(file_name, 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter='\t')
            for record in reader:
                try:
                    start_time = time.time()
                    record_dict = record
                    if len(record_dict) != 5:
                        raise ValueError("Invalid input formart")
                    data = {}
                    data["batch_id"] = batch_id
                    data["item_id"] = record_dict[0]
                    data["trusted_prediction"] = [record_dict[1]]
                    data["title"] = BeautifulSoup(record_dict[2]).get_text()
                    data["short_description"] = BeautifulSoup(record_dict[3]).get_text()
                    data["long_description"] = BeautifulSoup(record_dict[4]).get_text()
                    req_data = json.dumps(data)
                    response = requests_session.post(url, headers=headers, data=req_data, timeout=(10, 20), verify=False)

                    if response.ok:
                        response_body_sucess.append(response.content)
                    else:
                        response_body_failure.append(record)
                    lapse_time = time.time() - start_time
                    cnt_to_print_log += 1
                    print ("record_cnt - %s - response :%s - time elaplsed: %s" % (
                    cnt_to_print_log, response.content,
                    lapse_time))

                except Exception as e:
                    response_body_failure.append(record)
                    print("Exception %s" % e.message)
    except Exception as e:
        print("Error %s" % e)

        exit(1)

    lapse_time_main = time.time() - start_time_main

    print("Time taken to run the program : %s" % lapse_time_main)
    print "success_cnt - %s" % len(response_body_sucess)
    print "failure_cnt - %s" % len(response_body_failure)
    print  "Please use this link to get ae-measure:  http://{}:5003/pr/{}/{}".format(host_ip, batch_id, attribute)