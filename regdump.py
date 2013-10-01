#!/usr/bin/python
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

parser = argparse.ArgumentParser(description='Query registro-publico.gob.pa for sociedades.')
parser.add_argument('query', metavar='QUERY', type=str, help='text for query')

# create logger
logging.config.dictConfig(yaml.load(open('logging.yaml','r').read()))
logger = logging.getLogger('regdump')
logger.info('regdump started')

args = parser.parse_args()

sociedades = crawler.collect_query(args.query)
logger.info('found %i sociedades', len(sociedades))
logger.info('initializing data mining')
socidedades = crawler.scrape_sociedades(sociedades)
logger.info('found %i personas', len([item for sublist in [sociedad.personas for sociedad in socidedades] for item in sublist]))
logger.info('regdump finished')



