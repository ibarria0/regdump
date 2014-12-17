#!/usr/bin/python3
import sys
import argparse
import os
import logging.config
import yaml
import Classes
from modules import parser
from modules import crawler

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

# create logger
logging.config.dictConfig(yaml.load(open('logging.yaml','r').read()))
logger = logging.getLogger('regdump')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Query registro-publico.gob.pa for sociedades.')
    parser.add_argument('--query', dest='query', type=str)
    parser.add_argument('--start', dest='start', type=int)
    parser.add_argument('--stop', dest='start', type=int)
    parser.add_argument('--step', dest='step', type=str)
    args = parser.parse_args()

    logger.info('regdump started')

    if args.query:
        logger.info('performing query: %s', str(args.query))
        sociedades = crawler.query(args.query)
    else:
        sociedades = crawler.brute_fundaciones(False,args.start,args.stop,args.step)

    logger.info('found %i sociedades', len(sociedades))
    logger.info('found %i personas', len([item for sublist in [sociedad.personas for sociedad in sociedades] for item in sublist]))
    logger.info('regdump finished')



