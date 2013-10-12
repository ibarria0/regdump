from threading import Thread
from modules import db_worker,parser
from Classes import Sociedad
from time import sleep
from bs4 import BeautifulSoup,SoupStrainer

def dump_queue(queue):
    """Empties all pending items in a queue and returns them in a list."""
    result = []
    while queue.qsize() > 0:
        result.append(queue.get())
        queue.task_done()
    return result

def parse_sociedad_html(html):
    soup = BeautifulSoup(html, 'html.parser', parse_only=SoupStrainer('table'))
    sociedad = Sociedad(parser.collect_nombre(soup),parser.collect_ficha(soup))
    scrape_sociedad_data(sociedad,soup)
    asociaciones = scrape_sociedad_personas(sociedad,soup)
    return resolve_sociedad(sociedad,asociaciones)

def resolve_sociedad(sociedad,asociaciones):
    sociedad = db_worker.find_or_create_sociedades([sociedad])[0]
    for rol,personas in asociaciones.iteritems():
        if not isinstance(personas, tuple):
            for persona in personas:
                db_worker.find_or_create_asociaciones(persona,sociedad,rol) #create associations
        else:
            for persona_tuple in personas:
                db_worker.find_or_create_asociaciones(persona_tuple[1],sociedad,persona_tuple[0]) #create associations
    return sociedad

def scrape_sociedad_personas(sociedad,html):
  try:
    directores = scrape_sociedad_directores(sociedad,html)
    subscriptores = scrape_sociedad_subscriptores(sociedad,html)
    dignatarios = scrape_sociedad_dignatarios(sociedad,html)
  except:
    return {}
  return {'directores':directores, 'subscriptores':subscriptores, 'dignatarios':dignatarios}

def scrape_sociedad_directores(sociedad,html):
  directores = [Classes.Persona(persona) for persona in parser.collect_directores(html)]
  #directores = db_worker.find_or_create_asociaciones(directores,sociedad,'director') #create associations
  return directores

def scrape_sociedad_agente(sociedad,html):
  agente = Classes.Persona(parser.collect_agente(html))
  #agente = db_worker.find_or_create_asociaciones([agente],sociedad,'agente') #create associations
  return agente

def scrape_sociedad_subscriptores(sociedad,html):
  subscriptores = [Classes.Persona(persona) for persona in parser.collect_subscriptores(html)]
  #subscriptores = db_worker.find_or_create_asociaciones(subscriptores,sociedad,'subscriptor')
  return subscriptores

def scrape_sociedad_dignatarios(sociedad,html):
  dignatarios = parser.collect_dignatarios(html)
  for pair in dignatarios: pair[1] = Classes.Personas(pair[1])
  #for dignatario in parser.collect_dignatarios(html):
  #  dignatarios.extend(db_worker.find_or_create_asociaciones(dignatario[1],sociedad,dignatario[0]))
  #return dignatarios
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

class ProcessHTML(Thread):
  def __init__(self,html_queue):
    Thread.__init__(self)
    self.html_queue = html_queue

  def run(self):
    started = False
    while True:
        try:
            html = self.html_queue.get(timeout=2)
            parse_sociedad_html(html)
            self.html_queue.task_done()
            started = True
        except Exception as e:
            if started == True:
                return
            else:
                sleep(0.5)
                continue

