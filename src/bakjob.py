#!/usr/bin/python

import socket
import time
import logging

from urlparse import urlparse
from subprocess import call

import argparse


parser = argparse.ArgumentParser(description='Runs jobs regularly when certain cloud services or paths become available.')
parser.add_argument('--configfile', type=str, default="bakjob.conf", help='Config file')
parser.add_argument('--logfile', type=str, default="bakjob.log", help='Log file')
parser.add_argument('--quiet', action='store_true', help='Do not log to console')
parser.add_argument('--verbose', '-v', action='count', help='Verbosity level')
parser.add_argument('--sleeptime', type=int, default=600, help='Time to sleep in seconds between checks if there is work.')
args = parser.parse_args()

# parameters
sleep_time = args.sleeptime


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

# Set up logging
logger = logging.getLogger('bakjob')
logger.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

if not args.quiet:
    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG) # TODO make variable
    # add formatter to ch
    ch.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(ch)

if args.logfile:
    fh = logging.FileHandler(args.logfile)
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)






# 1st test job
bak_target   = 'www://www.fzenke.net'
bak_cmd      = 'ping -c 3 fzenke.net'
bak_interval = 3600 # a much larger interval in seconds
# bak_path = 'file:///home/zenke/bak'
job = { 'target' : bak_target, 'cmd' : bak_cmd, 'interval' : bak_interval }
bakjobs = [ job ]

# 2nd test job
bak_target   = 'rsync://fzenke.net/bak/arthur'
bak_cmd      = '/home/zenke/bin/backup_personal.sh'
bak_interval = 3600*12 # a much larger interval in seconds
job = { 'target' : bak_target, 'cmd' : bak_cmd, 'interval' : bak_interval }

bakjobs.append(job)


logger.info("bakjob started with %i jobs in conduit"%len(bakjobs))

while True:
    for jobid,job in enumerate(bakjobs):
        time.sleep(sleep_time)

        logger.debug("Checking task %i"%jobid)
        if 'last_run_time' in job.keys():
            next_run_time = job['last_run_time']+job['interval']
            if time.time()<next_run_time:
                logger.debug("No work - sleeping for %is"%sleep_time)
                continue

        url = urlparse(job['target'])
        if url.scheme == 'file':
            logger.warning("Do something locally")
        else:
            # print url.scheme
            # print url.hostname
            # print url.port
            if check_host_availability(url.hostname, url.port):
                logger.info("Host %s is available, running job %s: %s"%(url.hostname, jobid, job['cmd']))
                errorcode = call(job['cmd'], shell=True)
                if errorcode:
                    logger.error("The program returned the following error code: %i"%errorcode)
                else:
                    logger.info("Job %i finished sucessfully. Next run in >%is from now."%(jobid,job['interval']))
                    job['last_run_time'] = time.time()


