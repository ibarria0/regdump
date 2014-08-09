import requests
from Classes import Sociedad,Persona,Asociacion
from queue import Empty
import logging
import Classes
from modules import parser,db_worker
from bs4 import BeautifulSoup,SoupStrainer
from queue import Queue
from Query import Query,SociedadQuery,FichaQuery
from Worker import ProcessHTML
import Worker
from time import sleep
from threading import active_count
from urllib3 import HTTPConnectionPool,ProxyManager
import asyncio
from urllib3 import make_headers
import aiohttp

logger = logging.getLogger('crawler')
sem = asyncio.Semaphore(1)

def query_url(page,query):
    return ('http://201.224.39.199/scripts/nwwisapi.dll/conweb/MESAMENU?TODO=MER4&START=%s&FROM=%s' % (str(page),query))

def ficha_url(ficha):
    return ('http://201.224.39.199/scripts/nwwisapi.dll/conweb/MESAMENU?TODO=SHOW&ID=%s' % str(ficha))

def ficha_generator(old_fichas):
    for i in range(600000,999999):
        if i not in old_fichas:
            yield i
        else:
            continue

def brute_sociedades(fichas=False,skip_old=True):
    if skip_old:
        old_fichas = db_worker.get_fichas()
        fichas = ficha_generator(old_fichas)
    elif not fichas:
        fichas = range(0,10000000)
    loop = asyncio.get_event_loop()
    queue=[]
    f = asyncio.wait([get_html(url,queue,parse_sociedad_html) for url in fichas])
    loop.run_until_complete(f)
    return True


def generate_urls(url):
    page = 0
    while True:
        yield ( query_url(i,url)for i in range(page*15,(page+1)*15,15) )
        page += 1

@asyncio.coroutine
def get(*args, **kwargs):
    response = yield from aiohttp.request('GET', *args, **kwargs)
    return (yield from response.read())


@asyncio.coroutine
def get_html(url,queue,parser):
    headers=make_headers(user_agent="Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36")
    conn = aiohttp.ProxyConnector(proxy="http://localhost:8118")
    with (yield from sem):
        body = yield from get(url, headers=headers)
    sleep(3)
    [queue.append(i) for i in parser(body)]

def query_registro_publico(query):
  old = set()
  urls = generate_urls(query)
  loop = asyncio.get_event_loop()
  results = set()
  logger.info('initializing queries with %i threads', THREADS)
  while True:
    queue = []
    f = asyncio.wait([get_html(url,queue,parse_query_result) for url in next(urls)])
    loop.run_until_complete(f)
    results.update(queue)
    if results == old:
        return list(filter(None,results))
    else:
        old = results

def query(query):
    fichas = query_registro_publico(query)
    return brute_sociedades((ficha_url(f) for f in fichas),False)

def parse_query_result(html):
    soup = BeautifulSoup(html,"lxml", parse_only=SoupStrainer('table'),from_encoding='latin-1')
    sociedades = soup.find('th',text='NOMBRE SOCIEDAD').parent.next_siblings
    fichas = [int(row.find('a').string) for row in sociedades if row.td.string is not None]
    return fichas

def parse_sociedad_html(html):
    html = html.decode('ISO-8859-1','ignore')
    if parser.exists(html):
        soup = BeautifulSoup(html, 'html.parser', parse_only=SoupStrainer('p'))
        sociedad = scrape_sociedad_data(soup)
        logger.debug('found sociedad %s', sociedad.nombre)
        personas,asociaciones = scrape_personas(soup)
        logger.debug('found %i personas', len(personas))
        logger.debug('found %i asociaciones', len(asociaciones))
        return db_worker.resolve_sociedad(sociedad,personas,asociaciones)

def scrape_personas(soup):
    directores = scrape_sociedad_directores(soup)
    subscriptores = scrape_sociedad_subscriptores(soup)
    dignatarios = scrape_sociedad_dignatarios(soup)
    personas = set().union(*[directores, unpack_personas_in_dignatarios(dignatarios), subscriptores])
    asociaciones = dict(list({'directores':directores, 'subscriptores':subscriptores}.items()) + list(dignatarios.items()))
    return [personas,asociaciones]

def scrape_sociedad_directores(soup):
    try:
        directores = {Persona(persona) for persona in parser.collect_directores(soup)}
    except Exception as e:
        print(e)
        return set()
    return directores

def scrape_sociedad_subscriptores(soup):
    try:
        subscriptores = {Persona(persona) for persona in parser.collect_subscriptores(soup)}
    except Exception as e:
        print(e)
        return {}
    return subscriptores

def scrape_sociedad_dignatarios(soup):
  try:
      dignatarios = { item[0]: {Persona(value) for value in item[1]} for item in parser.collect_dignatarios(soup).items()}
  except Exception as e:
    print(e)
    return {}
  return dignatarios

def unpack_personas_in_dignatarios(dignatarios):
    return {val for sublist in dignatarios.values() for val in sublist}

def scrape_sociedad_data(soup):
    sociedad = Sociedad(parser.collect_nombre(soup),parser.collect_ficha(soup))
    sociedad.fecha_registro = parser.collect_fecha_registro(soup)
    sociedad.provincia = parser.collect_provincia(soup)
    sociedad.notaria = parser.collect_notaria(soup)
    sociedad.duracion = parser.collect_duracion(soup)
    sociedad.status = parser.collect_status(soup)
    sociedad.agente = parser.collect_agente(soup)
    sociedad.moneda = parser.collect_moneda(soup)
    sociedad.capital = parser.collect_capital(soup)
    sociedad.capital_text = parser.collect_capital_text(soup)
    sociedad.representante_text = parser.collect_representante_text(soup)
    return sociedad

