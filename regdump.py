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

def query(query):
  return crawler.collect_query()

def scape_sociedades(sociedades):
  return crawler.scrape_sociedades(sociedades)

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Query registro-publico.gob.pa for sociedades.')
  parser.add_argument('query', metavar='QUERY', type=str, help='text for query')
  parser.add_argument('--threads', dest='threads', type=int, default=15)
  args = parser.parse_args()
  crawler.setThreads(args.threads) #set threads

  logger.info('regdump started')
  logger.info('performing query: %s', str(args.query))
  sociedades = crawler.collect_query(args.query)
  logger.info('found %i sociedades', len(sociedades))
  if len(sociedades) > 0:
    socidedades = crawler.scrape_sociedades(sociedades)
    logger.info('found %i personas', len([item for sublist in [sociedad.personas for sociedad in socidedades] for item in sublist]))
  logger.info('regdump finished')



