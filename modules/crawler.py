import requests
import logging
import Classes
from modules import parser,db_worker
from bs4 import BeautifulSoup,SoupStrainer
from queue import Queue
from Query import Query,SociedadQuery,FichaQuery
from Worker import ProcessHTML
from time import sleep
from threading import active_count
from urllib3 import HTTPConnectionPool,ProxyManager

logger = logging.getLogger('crawler')

def query_url(page,query):
    return ('/scripts/nwwisapi.dll/conweb/MESAMENU?TODO=MER4&START=%s&FROM=%s' % (str(page),query))

def dump_queue(queue):
    """Empties all pending items in a queue and returns them in a list."""
    result = []
    while queue.qsize() > 0:
        result.append(queue.get())
        queue.task_done()
    return result

def setThreads(n):
    global THREADS
    global pool
    pool = ProxyManager('http://localhost:8118', num_pools=n)
    THREADS = n 

def spawn_html_processing_thread(html_queue):
    thread = ProcessHTML(html_queue)
    thread.setDaemon(True)
    thread.start()
    return thread

def brute_sociedades(iterator=range(db_worker.find_max_ficha(),10000000),skip_old=True):
    if skip_old:
        old_fichas = db_worker.get_fichas()
        fichas = [ficha for ficha in iterator if ficha not in old_fichas]
    else:
        fichas = list(iterator)
    if len(fichas) > 0:
        fichas_iter = iter(fichas)
        html_queue = Queue()
        processing_thread = spawn_html_processing_thread(html_queue)
        while True:
            try:
                spawn_ficha_queries(fichas_iter,html_queue,THREADS - active_count() + 1)
                sleep(0.1)
            except StopIteration:
                while active_count() > 2: sleep(0.1)
                break
            except AttributeError:
                continue
        logger.info('done scraping - waiting on processing')
        processing_thread.join()
    return db_worker.find_by_fichas(fichas)

def spawn_ficha_queries(fichas,html_queue,n):
    for i in range(n):
        query = FichaQuery(next(fichas),html_queue,pool)
        query.setDaemon(True)
        query.start()

def query_registro_publico(query):
  page = 1
  html_queue = Queue()
  results = []
  logger.info('initializing queries with %i threads', THREADS)
  while (len(results) % 15 == 0 ):
    queries,page = spawn_queries(query,(THREADS - active_count() + 1),page,html_queue) #fill thread pool
    results.extend(process_html_queue(html_queue))
    if not all(results): break #there is a false in results so this is done
    sleep(0.1)
  while any([query.is_alive() for query in queries]): sleep(1) #wait for pending threads
  results.extend(process_html_queue(html_queue))
  return [item for sublist in results for item in sublist if item is not False]

def query(query):
    fichas = query_registro_publico(query)
    return brute_sociedades(iter(fichas),False)
     
def spawn_queries(query,n,start_page,html_queue):
  queries = [Query(query_url(i,query),html_queue,pool) for i in range(start_page,start_page+(15*n),15)]
  for query in queries: 
    query.setDaemon(True)
    query.start()
  return [queries,start_page + (15*n)]

def parse_query_result(html):
    soup = BeautifulSoup(html,"html.parser", parse_only=SoupStrainer('table'),from_encoding='latin-1')
    sociedades = soup.find('th',text='NOMBRE SOCIEDAD').parent.next_siblings
    fichas = [int(row.find('a').string) for row in sociedades if row.td.string is not None]
    return fichas

def process_html_queue(html_queue):
    return [parse_query_result(html) for html in dump_queue(html_queue)]

def spawn_sociedad_queries(sociedades,n,sociedades_queue):
    for i in range(n):
        query = SociedadQuery(next(sociedades),sociedades_queue,html_queue,pool)
        query.setDaemon(True)
        query.start()
        queries.append(query)
