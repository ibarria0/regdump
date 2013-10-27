from threading import Thread
from modules import db_worker,parser
from Classes import Sociedad,Persona,Asociacion
from time import sleep
from bs4 import BeautifulSoup,SoupStrainer
from queue import Empty


def parse_sociedad_html(html):
    soup = BeautifulSoup(html, 'html.parser', parse_only=SoupStrainer('p'),from_encoding='latin-1')
    sociedad = scrape_sociedad_data(soup)
    personas,asociaciones = scrape_personas(soup)
    return resolve_sociedad(sociedad,personas,asociaciones)

def resolve_sociedad(sociedad,personas,asociaciones):
    sociedad = db_worker.find_or_create_sociedades([sociedad])[0]
    personas = db_worker.find_or_create_personas(personas)
    asociaciones = resolve_asociaciones(personas,asociaciones)
    for rol in asociaciones.keys():
        db_worker.find_or_create_asociaciones(asociaciones[rol],sociedad,rol) #create associations
    print(sociedad.nombre,len(sociedad.personas))
    return sociedad

def resolve_asociaciones(personas,asociaciones):
    personas = list(personas)
    for key in asociaciones.keys():
        tmp = set() 
        for persona in asociaciones[key]:
            tmp.add(personas[personas.index(persona)])
        asociaciones[key] = tmp
    return asociaciones 

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
            html = self.html_queue.get(timeout=10)
            parse_sociedad_html(html)
            self.html_queue.task_done()
            started = True
        except Empty:
            if started == True:
                return
        except Exception as e:
                print(e)
                sleep(0.1)

