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
    parser.add_argument('--start', dest='start', type=int)
    parser.add_argument('--stop', dest='stop', type=int)
    parser.add_argument('--step', dest='step', type=int)
    args = parser.parse_args()

    logger.info('regdump started')
    sociedades = crawler.brute_sociedades(args.start,args.stop,args.step)

    logger.info('found %i sociedades', len(sociedades))
    logger.info('found %i personas', len([item for sublist in [sociedad.personas for sociedad in sociedades] for item in sublist]))
    logger.info('regdump finished')
