import requests
import logging
import Classes
from modules import parser,db_worker
from bs4 import BeautifulSoup,SoupStrainer
from Queue import Queue
from Query import Query,SociedadQuery,FichaQuery
from Worker import ProcessHTML
from time import sleep
from threading import active_count
from urllib3 import HTTPConnectionPool

logger = logging.getLogger('crawler')

def setThreads(n):
    global THREADS
    global pool
    pool = HTTPConnectionPool('201.224.39.199', maxsize=n)
    THREADS = n 

def spawn_html_processing_thread(html_queue):
    thread = ProcessHTML(html_queue)
    thread.setDaemon(True)
    thread.start()
    return thread

def brute_sociedades():
    old_fichas = db_worker.get_fichas()
    fichas = {ficha for ficha in xrange(5000) if ficha not in old_fichas}
    if len(fichas) > 0:
        fichas = iter(fichas)
        html_queue = Queue()
        processing_thread = spawn_html_processing_thread(html_queue)
        while True:
            try:
                spawn_ficha_queries(fichas,html_queue,THREADS - active_count() + 1)
                sleep(0.1)
            except StopIteration:
                while active_count() > 2: sleep(0.1)
                break
            except AttributeError:
                continue
        logger.info('done scraping - waiting on processing')
        processing_thread.join()

def spawn_ficha_queries(fichas,html_queue,n):
    for i in xrange(n):
        query = FichaQuery(fichas.next(),html_queue,pool)
        query.setDaemon(True)
        query.start()
 
