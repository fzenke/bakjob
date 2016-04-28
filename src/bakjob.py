#!/usr/bin/python

import socket
import time
import logging

from urlparse import urlparse
from subprocess import call



logger = logging.getLogger('bakjob')
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)

def check_host_availability(hostname, port=22):
    if port == None:
        port = 22
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((hostname, port))
        logger.info("Port %i on %s reachable"%(port, hostname))
        return_value = True
    except socket.error as e:
        return_value = False
        logger.info("Error on connect: %s" % e)
    s.close()
    return return_value

def check_path_availability(path):
    # TODO implement 
    return True

bak_target   = 'rsync://fzenke.net/bak/arthur'
bak_cmd      = 'ping -c 3 fzenke.net'
bak_interval = 10 # a much larger interval ideally something in hours or days, but given in secs
# bak_path = 'file:///home/zenke/bak'

bakjobs = [ { 'target' : bak_target, 'cmd' : bak_cmd, 'interval' : bak_interval }  ]
# print tasks

sleep_time = 5 # relatively small interval to check for work in the queue say 600s

while True:
    for i,bakjob in enumerate(bakjobs):
        time.sleep(sleep_time)

        logger.info("Checking task %i"%i)
        if 'last_run_time' in bakjob.keys():
            next_run_time = bakjob['last_run_time']+bakjob['interval']
            if time.time()<next_run_time:
                logger.debug("No work - sleeping for %is"%sleep_time)
                continue

        url = urlparse(bakjob['target'])
        if url.scheme == 'file':
            logger.warning("Do something locally")
        else:
            print url.scheme
            print url.hostname
            print url.port
            if check_host_availability(url.hostname, url.port):
                logger.info("Host %s is available, running bakjob"%url.hostname)
                errorcode = call(bakjob['cmd'], shell=True)
                if errorcode:
                    logger.error("The program returned the following error code: %i"%errorcode)
                bakjob['last_run_time'] = time.time()
