import requests
import logging
import Classes
from modules import parser,db_worker
from bs4 import BeautifulSoup,SoupStrainer
from Queue import Queue
from Query import Query,SociedadQuery
from time import sleep
from threading import active_count
from multiprocessing import Pool
from urllib3 import HTTPConnectionPool

only_row_tags = SoupStrainer("tr")
a_table = SoupStrainer("table")
consulta = SoupStrainer(id="CONSULTA")
logger = logging.getLogger('crawler')

def query_url(page,query):
  return ('/scripts/nwwisapi.dll/conweb/MESAMENU?TODO=MER4&START=%s&FROM=%s' % (str(page),query))

def parse_query_result(html):
  try:
    sociedades = BeautifulSoup(html,"html.parser", parse_only=a_table).find('th',text='NOMBRE SOCIEDAD').parent.next_siblings
    sociedad_objects = [Classes.Sociedad(unicode(row.td.string), int(row.find('a').string)) for row in sociedades if row.td.string is not None]
  except AttributeError:
    logger.info('failed to retrieve sociedades')
  if len(sociedad_objects) > 0:
    return sociedad_objects
  else:
    return [False]

def dump_queue(queue):
    """Empties all pending items in a queue and returns them in a list."""
    result = []
    while queue.qsize() > 0:
        result.append(queue.get())
        queue.task_done()
    return result

def chunks(l, n):
    """ Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield filter(lambda x: x is not None,l[i:i+n]) 

def setThreads(n):
    global THREADS
    global pool
    pool = HTTPConnectionPool('201.224.39.199', maxsize=n)
    THREADS = n 

def collect_query(query):
  page = 1
  html_queue = Queue()
  results = []
  logger.info('initializing queries with %i threads', THREADS)
  while (len(results) % 15 == 0 ):
    queries,page = spawn_queries(query,(THREADS - active_count() + 1),page,html_queue) #fill thread pool
    for html in dump_queue(html_queue): results.extend(parse_query_result(html))  #process pending
    sleep(0.1)
    if not all(results): break #there is a false in results so this is done
  while any([query.is_alive() for query in queries]): sleep(1) #wait for pending threads
  for html in dump_queue(html_queue): results.extend(parse_query_result(html)) #process pending
  results = filter(lambda x: x is not False,results)
  results = filter(lambda x: int(x.ficha) > 0, results)
  return db_worker.find_or_create_sociedades(results)

def spawn_queries(query,n,start_page,html_queue):
  queries = [Query(query_url(i,query),html_queue,pool) for i in xrange(start_page,start_page+(15*n),15)]
  for query in queries: 
    query.setDaemon(True)
    query.start()
  return [queries,start_page + (15*n)]

def scrape_sociedad(sociedad):
  try:
    scrape_sociedad_data(sociedad,sociedad.html)
    scrape_sociedad_personas(sociedad,sociedad.html)
  except:
    pass
  finally:
    sociedad.html = None
    return True
 
def scrape_sociedades(sociedades):
  logger.info('initializing data mining with %i threads', THREADS)
  sociedades_stack = list(filter(lambda x: not x.visited,sociedades)) #copy list
  sociedades_queue = Queue()
  queries = []
  while sociedades_stack:
    queries.extend(spawn_sociedad_queries(sociedades_stack,(THREADS - active_count() + 1),sociedades_queue)) #fill thread pool
    sleep(0.1)
  while any([query.is_alive() for query in queries]): sleep(1)
  process_sociedades_queue(sociedades_queue)
  return db_worker.mark_sociedades_visited(sociedades)

def process_sociedades_queue(sociedades_queue):
  for sociedad in dump_queue(sociedades_queue): scrape_sociedad(sociedad)

def spawn_sociedad_queries(sociedades,n,sociedades_queue):
  queries = []
  for i in xrange(n):
    try:
      query = SociedadQuery(sociedades.pop(),sociedades_queue,pool)
      query.setDaemon(True)
      query.start()
      queries.append(query)
    except IndexError:
      break
  return queries
      

def scrape_sociedad_personas(sociedad,html):
  try:
    directores = scrape_sociedad_directores(sociedad,html)
    subscriptores = scrape_sociedad_subscriptores(sociedad,html)
    dignatarios = scrape_sociedad_dignatarios(sociedad,html)
  except:
    return set()
  return set(directores + subscriptores + dignatarios) 

def scrape_sociedad_directores(sociedad,html):
  directores = [Classes.Persona(persona) for persona in parser.collect_directores(html)]
  directores = db_worker.find_or_create_asociaciones(directores,sociedad,'director') #create associations
  return directores

def scrape_sociedad_agente(sociedad,html):
  agente = Classes.Persona(parser.collect_agente(html))
  agente = db_worker.find_or_create_asociaciones([agente],sociedad,'agente') #create associations
  return agente

def scrape_sociedad_subscriptores(sociedad,html):
  subscriptores = [Classes.Persona(persona) for persona in parser.collect_subscriptores(html)]
  subscriptores = db_worker.find_or_create_asociaciones(subscriptores,sociedad,'subscriptor')
  return subscriptores

def scrape_sociedad_dignatarios(sociedad,html):
  dignatarios = []
  for dignatario in parser.collect_dignatarios(html):
    dignatarios.extend(db_worker.find_or_create_asociaciones([Classes.Persona(dignatario[1])],sociedad,dignatario[0]))
  return dignatarios

def scrape_sociedad_data(sociedad,html):
  try:
    sociedad.fecha_registro = parser.collect_fecha_registro(html)
    sociedad.provincia = parser.collect_provincia(html)
    sociedad.notaria = parser.collect_notaria(html)
    sociedad.duracion = parser.collect_duracion(html)
    sociedad.stats = parser.collect_status(html)
    sociedad.agente = parser.collect_agente(html)
    sociedad.moneda = parser.collect_moneda(html)
    sociedad.capital = parser.collect_capital(html)
    sociedad.capital_text = parser.collect_capital_text(html)
    sociedad.representante_text = parser.collect_representante_text(html)
  except:
    return sociedad
  return sociedad
