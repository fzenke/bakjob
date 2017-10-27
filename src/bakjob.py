#!/usr/bin/python3

# This is a copy of BAKJOB a backup scheduling tool for computers 
# with sporadic access to certain backup media such as USB devices
# or the internet.
#
#
# Copyright (c) 2016 Friedemann Zenke
#
# BAKJOB is distributed under the MIT license.
# See LICENSE for details.

import os
import sys
import socket
import time
import datetime
import logging
import argparse
import configparser
import pickle
import hashlib

from urllib.parse import urlparse
from subprocess import call
from tendo import singleton

# Only run a single instance of bakjob
me = singleton.SingleInstance() # will sys.exit(-1) if other instance is running

# Parse command line arguments
parser = argparse.ArgumentParser(description='Runs jobs regularly when certain cloud services or paths become available.')
parser.add_argument('--configfile', type=str, default="bakjob.conf", help='Config file')
parser.add_argument('--logfile', type=str, default="bakjob.log", help='Log file')
parser.add_argument('--statefile', type=str, default="bakjob.dat", help='Save file for last run times')
parser.add_argument('--quiet', action='store_true', help='Do not log to console')
parser.add_argument('--last', action='store_true', help='List last backup times and exit')
parser.add_argument('--verbose', '-v', action='count', help='Verbosity level')
parser.add_argument('--sleeptime', '-s', type=int, default=600, help='Time to sleep in seconds between checks if there is work.')
args = parser.parse_args()

# Set up logging
logger = logging.getLogger('bakjob')

# create formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# loglevel
loglevel = logging.INFO
if args.verbose:
    loglevel = logging.DEBUG
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)


if not args.quiet:
    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(loglevel) 
    # add formatter to ch
    ch.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(ch)

if args.logfile:
    fh = logging.FileHandler(args.logfile)
    fh.setLevel(loglevel)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

def convert_seconds_to_human_readible(c):
    days = c // 86400
    hours = c // 3600 % 24
    minutes = c // 60 % 60
    seconds = c % 60

    if days:
        return "%i days"%days
    elif days:
        return "%ih"%hours
    else:
        return "%imin %is"%(minutes, seconds)

# Load run data
try:
    pkl_file = open(args.statefile, 'rb')
    rundata = pickle.load(pkl_file)
    pkl_file.close()
except IOError:
    rundata = {}
    logger.warning("Could not open statefile %s"%args.statefile)


# parse config file
bakjobs = [ ]
config = configparser.RawConfigParser()
config.read(args.configfile)
for section in config.sections():
    m = hashlib.md5()
    m.update(config.get(section,'cmd').__str__().encode('utf-8'))
    md5hash = m.digest()
    last_run_time = 0
    if md5hash in rundata.keys():
        last_run_time = rundata[md5hash]
        date_and_time = datetime.datetime.fromtimestamp(last_run_time).strftime('%Y-%m-%d %H:%M:%S')
        logger.debug("Job %s ran last on %s"%(section, date_and_time ))
    else:
        logger.debug("No last runtime for %s saved, scheduled for running"%section)
    job = { 'name'     : section, 
            'target'   : config.get(section,'target') , 
            'cmd'      : config.get(section,'cmd') , 
            'interval' : config.getint(section,'interval') ,
            'md5hash'  : md5hash ,
            'last_run_time' : last_run_time ,
            'urlinfo'  : urlparse(config.get(section,'target') )
            }
    bakjobs.append(job)

# List the last runtimes of all jobs
if args.last:
    print("Last run jobs")
    for jobid,job in enumerate(bakjobs):
        if 'last_run_time' in job.keys():
            date_and_time_str = datetime.datetime.fromtimestamp(job['last_run_time']).strftime('%Y-%m-%d %H:%M:%S')
            print("Job %i: %s (%s)"%(jobid,date_and_time_str,job['name']))
    sys.exit(0)

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
        logger.debug("Error when tyring to connect to %s: %s" % (hostname, e))
    s.close()
    return return_value


def check_path_availability(path):
    return os.path.exists(path)

def save_last_run_times(jobs):
    data = {}
    for job in jobs:
        data[job['md5hash']] = job['last_run_time']

    try:
        pkl_file = open(args.statefile, 'wb')
        pickle.dump(data, pkl_file)
        pkl_file.close()
    except IOError:
        rundata = {}
        logger.warning("Could not open statefile %s for writing"%args.statefile)

def run_job(job):
    errorcode = call(job['cmd'], shell=True)
    if errorcode:
        logger.error("The program returned the following error code: %i"%errorcode)
    else:
        logger.info("Job %i finished sucessfully. Next check delayed by %s."%(jobid, convert_seconds_to_human_readible(job['interval'])))
        job['last_run_time'] = time.time()
        save_last_run_times(bakjobs)



if len(bakjobs):
    logger.info("bakjob started with %i jobs in conduit"%len(bakjobs))
else:
    logger.error("No jobs in config file! Exiting.")
    sys.exit(1)

try:
    while True:
        for jobid,job in enumerate(bakjobs):

            logger.debug("Checking job %i (%s)"%(jobid, job['name']))
            if 'last_run_time' in job.keys():
                next_run_time = job['last_run_time']+job['interval']
                if time.time()<next_run_time:
                    logger.debug("No work - sleeping for %is"%args.sleeptime)
                    continue

            url = job['urlinfo']
            if url.scheme == 'file':
                if check_path_availability(url.path):
                    logger.info("Path %s is available, running job %i (%s)"%(url.path, jobid, job['name']))
                    run_job(job)
                else:
                    logger.debug("Path %s is not available for job %i (%s), waiting ..."%(url.path, jobid, job['name']))

            else:
                if check_host_availability(url.hostname, url.port):
                    logger.info("Host %s is available, running job %s: %s"%(url.hostname, jobid, job['name']))
                    run_job(job)
                else:
                    logger.debug("Host %s is unavailable for job %i (%s), waiting ..."%(url.hostname, jobid, job['name']))

        time.sleep(args.sleeptime)
except KeyboardInterrupt:
    logger.info("Exiting")
