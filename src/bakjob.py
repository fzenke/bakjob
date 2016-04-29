#!/usr/bin/python

import sys
import socket
import time
import logging
import argparse
import md5
import ConfigParser
import pickle

from urlparse import urlparse
from subprocess import call



# Parse command line arguments
parser = argparse.ArgumentParser(description='Runs jobs regularly when certain cloud services or paths become available.')
parser.add_argument('--configfile', type=str, default="bakjob.conf", help='Config file')
parser.add_argument('--logfile', type=str, default="bakjob.log", help='Log file')
parser.add_argument('--statefile', type=str, default="bakjob.dat", help='Save file for last run times')
parser.add_argument('--quiet', action='store_true', help='Do not log to console')
parser.add_argument('--verbose', '-v', action='count', help='Verbosity level')
parser.add_argument('--sleeptime', type=int, default=600, help='Time to sleep in seconds between checks if there is work.')
args = parser.parse_args()
# parameters
sleep_time = args.sleeptime


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


# Load run data
#
try:
    pkl_file = open(args.statefile, 'rb')
    rundata = pickle.load(pkl_file)
    pkl_file.close()
except IOError:
    rundata = {}
    logger.warning("Could not open statefile %s"%args.statefile)

# parse config file
bakjobs = [ ]
config = ConfigParser.RawConfigParser()
config.read(args.configfile)
for section in config.sections():
    m = md5.new(config.get(section,'cmd').__str__())
    md5hash = m.digest()
    last_run_time = 0
    if md5hash in rundata.keys():
        last_run_time = rundata[md5hash]
        logger.debug("Loading runtime for %s"%section)
    else:
        logger.debug("No runtime for %s saved, scheduled for running"%section)
    job = { 'name'     : section, 
            'target'   : config.get(section,'target') , 
            'cmd'      : config.get(section,'cmd') , 
            'interval' : config.getint(section,'interval') ,
            'md5hash'  : md5hash ,
            'last_run_time' : last_run_time
            }
    bakjobs.append(job)

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

def save_last_run_times(jobs):
    data = {}
    for job in jobs:
        data[job['md5hash']] = job['last_run_time']

    try:
        pkl_file = open(args.statefile, 'w')
        pickle.dump(data, pkl_file)
        pkl_file.close()
    except IOError:
        rundata = {}
        logger.warning("Could not open statefile %s for writing"%args.statefile)



if len(bakjobs):
    logger.info("bakjob started with %i jobs in conduit"%len(bakjobs))
else:
    logger.error("No jobs in config file! Exiting.")
    sys.exit(1)

while True:
    for jobid,job in enumerate(bakjobs):
        time.sleep(sleep_time)

        logger.debug("Checking job %i (%s)"%(jobid, job['name']))
        if 'last_run_time' in job.keys():
            next_run_time = job['last_run_time']+job['interval']
            if time.time()<next_run_time:
                logger.debug("No work - sleeping for %is"%sleep_time)
                continue

        url = urlparse(job['target'])
        if url.scheme == 'file':
            # TODO Implement logic for paths
            logger.warning("Do something locally")
        else:
            if check_host_availability(url.hostname, url.port):
                logger.info("Host %s is available, running job %s: %s"%(url.hostname, jobid, job['cmd']))
                errorcode = call(job['cmd'], shell=True)
                if errorcode:
                    logger.error("The program returned the following error code: %i"%errorcode)
                else:
                    logger.info("Job %i finished sucessfully. Next run in >%is from now."%(jobid,job['interval']))
                    job['last_run_time'] = time.time()
                    save_last_run_times(bakjobs)


