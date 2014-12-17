import requests
from Classes import Sociedad,Persona,Asociacion,Fundacion,FundacionPersonas
from queue import Empty
import logging
import Classes
from Classes import *

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
from random import sample

logger = logging.getLogger('crawler')
sem = asyncio.Semaphore(1)
lock = asyncio.Lock()
user_agents = ['Mozilla/5.0 (Windows NT 5.1; rv:31.0) Gecko/20100101 Firefox/31.0', 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36' , 'Mozilla/5.0 (iPad; CPU OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5355d Safari/8536.25', 'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)' , 'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)', 'Mozilla/5.0 (iPad; CPU OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5355d Safari/8536.25','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/537.13+ (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2']
old_fichas = db_worker.get_fichas()

def query_url(page,query):
    return ('http://201.224.39.199/scripts/nwwisapi.dll/conweb/MESAMENU?TODO=MER4&START=%s&FROM=%s' % (str(page),query))

def ficha_url(ficha):
    return ('http://201.224.39.199/scripts/nwwisapi.dll/conweb/MEIPMENU?TODO=SHOW&ID=%s' % str(ficha))

def ficha_generator(fichas,old_fichas):
    for i in fichas:
        if i not in old_fichas:
            yield ficha_url(i)
        else:
            continue

def brute_fundaciones(fichas,start,stop,step):
    queue=[]
    if skip_old and isinstance(fichas,set):
        fichas = fichas.difference(old_fichas)
        if len(fichas) == 0:
           return queue
    elif not fichas:
        fichas = range(start,stop,step)
    lock = asyncio.Lock()
    loop = asyncio.get_event_loop()
    f = asyncio.wait([get_html(fc,queue,parse_fundacion_html) for fc in fichas])
    loop.run_until_complete(f)
    return queue

def brute_sociedades(fichas=False,skip_old=True):
    queue=[]
    if skip_old and isinstance(fichas,set):
        fichas = fichas.difference(old_fichas)
        if len(fichas) == 0:
           return queue
    elif not fichas:
        fichas = range(500000,1000000,5)
    lock = asyncio.Lock()
    loop = asyncio.get_event_loop()
    f = asyncio.wait([get_html(fc,queue,parse_sociedad_html) for fc in fichas])
    loop.run_until_complete(f)
    return queue

def generate_urls(url):
    page = 0
    while True:
        yield ( query_url(i,url)for i in range(page*15,(page+1)*15,15) )
        page += 1

@asyncio.coroutine
def get(*args, **kwargs):
    response = yield from aiohttp.request('GET', *args, **kwargs)
    print(response)
    return (yield from response.read())

@asyncio.coroutine
def get_html(url,queue,parser):
    headers=make_headers(user_agent=sample(user_agents,1)[0])
    #conn = aiohttp.ProxyConnector(proxy='http://localhost:8118')
    with (yield from sem):
        if parser != parse_query_result and url not in old_fichas:
            body = yield from get(ficha_url(url), headers=headers) #, connector=conn)
        elif parser == parse_query_result:
            body = yield from get(url, headers=headers) #, connector=conn)
        sleep(4)
    with (yield from lock):
        [queue.append(i) for i in parser(body)]

def query_registro_publico(query):
  old = set()
  urls = generate_urls(query)
  loop = asyncio.get_event_loop()
  results = set()
  while True:
    queue = []
    f = asyncio.wait([get_html(url,queue,parse_query_result) for url in next(urls)])
    loop.run_until_complete(f)
    results.update(queue)
    if results == old:
        return set(filter(None,results))
    else:
        old = results

def query(query):
    fichas = query_registro_publico(query)
    return brute_sociedades(fichas,True)

def parse_query_result(html):
    soup = BeautifulSoup(html,"lxml", parse_only=SoupStrainer('table'))
    sociedades = soup.find('th',text='NOMBRE SOCIEDAD').parent.next_siblings
    fichas = [int(row.find('a').string) for row in sociedades if row.td.string is not None]
    return fichas

def parse_fundacion_html(html):
    html = html.decode('ISO-8859-1','ignore')
    if parser.exists(html):
        soup = BeautifulSoup(html, 'html.parser', parse_only=SoupStrainer('p'))
        fundacion = scrape_fundacion_data(soup)
        logger.info('found fundacion %s', fundacion.nombre)
        personas,asociaciones = scrape_fundacion_personas(soup)
        logger.debug('found %i personas', len(personas))
        logger.debug('found %i asociaciones', len(asociaciones))
        return db_worker.resolve_fundacion(fundacion,personas,asociaciones)

def parse_sociedad_html(html):
    html = html.decode('ISO-8859-1','ignore')
    if parser.exists(html):
        soup = BeautifulSoup(html, 'html.parser', parse_only=SoupStrainer('p'))
        sociedad = scrape_sociedad_data(soup)
        logger.info('found sociedad %s', sociedad.nombre)
        personas,asociaciones = scrape_personas(soup)
        logger.debug('found %i personas', len(personas))
        logger.debug('found %i asociaciones', len(asociaciones))
        return db_worker.resolve_sociedad(sociedad,personas,asociaciones)

def scrape_fundacion_personas(soup):
    miembros = scrape_fundacion_miembros(soup)
    fundadores = scrape_fundacion_fundadores(soup)
    cargos = scrape_fundacion_cargos(soup)
    personas = set().union(*[miembros, unpack_personas_in_dignatarios(cargos), fundadores])
    asociaciones = dict(list({'miembros':miembros, 'fundadores':fundadores}.items()) + list(cargos.items()))
    return [personas,asociaciones]

def scrape_fundacion_miembros(soup):
    try:
        miembros = {Persona(persona) for persona in parser.collect_miembros(soup)}
    except Exception as e:
        print(e)
        return set()
    return miembros

def scrape_fundacion_fundadores(soup):
    try:
        fundadores = {Persona(persona) for persona in parser.collect_fundadores(soup)}
    except Exception as e:
        print(e)
        return {}
    return fundadores

def scrape_fundacion_cargos(soup):
  try:
      cargos = { item[0]: {Persona(value) for value in item[1]} for item in parser.collect_cargos(soup).items()}
  except Exception as e:
    print(e)
    return {}
  return cargos

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

def scrape_fundacion_data(soup):
    fundacion = Fundacion(parser.collect_nombre_fundacion(soup),parser.collect_ficha(soup))
    fundacion.escritura = parser.collect_escritura(soup)
    fundacion.documento = parser.collect_documento(soup)
    fundacion.fecha_registro = parser.collect_fecha_registro(soup)
    fundacion.provincia = parser.collect_provincia(soup)
    fundacion.notaria = parser.collect_notaria(soup)
    fundacion.duracion = parser.collect_duracion(soup)
    fundacion.status = parser.collect_status(soup)
    fundacion.agente = parser.collect_agente(soup)
    fundacion.moneda = parser.collect_moneda(soup)
    fundacion.patrimonio = parser.collect_patrimonio(soup)
    fundacion.patrimonio_text = parser.collect_patrimonio_text(soup)
    fundacion.firmante_text = parser.collect_firmante_text(soup)
    return fundacion

