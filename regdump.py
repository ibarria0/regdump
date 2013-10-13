#!/usr/bin/python2.7
import sys
import argparse
import os
import requests
import logging.config
import yaml
    
from itertools import izip
import Classes
import re
from modules import parser
from modules import crawler
from multiprocessing import Pool,cpu_count

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

# create logger
logging.config.dictConfig(yaml.load(open('logging.yaml','r').read()))
logger = logging.getLogger('regdump')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Query registro-publico.gob.pa for sociedades.')
    parser.add_argument('--query', dest='query', type=str)
    parser.add_argument('--threads', dest='threads', type=int, default=15)
    args = parser.parse_args()
    crawler.setThreads(args.threads) #set threads

    logger.info('regdump started')

    if args.query:
        logger.info('performing query: %s', str(args.query))
        sociedades = crawler.query(args.query)
    else:
        sociedades = crawler.brute_sociedades(skip_old=True)

    logger.info('found %i sociedades', len(sociedades))
    logger.info('found %i personas', len([item for sublist in [sociedad.personas for sociedad in sociedades] for item in sublist]))
    logger.info('regdump finished')



