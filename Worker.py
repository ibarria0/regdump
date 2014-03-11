from threading import Thread
from modules import db_worker,parser
from Classes import Sociedad,Persona,Asociacion
from time import sleep
from bs4 import BeautifulSoup,SoupStrainer
from queue import Empty
import logging

logger = logging.getLogger('worker')

def parse_sociedad_html(html):
    if parser.exists(html):
        soup = BeautifulSoup(html, 'html.parser', parse_only=SoupStrainer('p'))
        sociedad = scrape_sociedad_data(soup)
        logger.info('found sociedad %s', sociedad.nombre)
        personas,asociaciones = scrape_personas(soup)
        logger.info('found %i personas', len(personas))
        logger.info('found %i asociaciones', len(asociaciones))
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

class ProcessHTML(Thread):
  def __init__(self,html_queue):
    Thread.__init__(self)
    self.html_queue = html_queue

  def run(self):
    started = False
    while True:
        try:
            html = self.html_queue.get(timeout=20)
            parse_sociedad_html(html)
            self.html_queue.task_done()
            started = True
        except Empty:
            if started == True:
                raise Exception('worker is dead')
                return
        except Exception as e:
                print(e)
                sleep(0.1)

