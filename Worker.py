from threading import Thread
from modules import db_worker,parser
from Classes import Sociedad,Persona,Asociacion
from time import sleep
from bs4 import BeautifulSoup,SoupStrainer
from Queue import Empty

def parse_sociedad_html(html):
    soup = BeautifulSoup(html, 'html.parser', parse_only=SoupStrainer('p'))
    sociedad = Sociedad(parser.collect_nombre(soup),parser.collect_ficha(soup))
    scrape_sociedad_data(sociedad,soup)
    asociaciones = scrape_sociedad_personas(sociedad,soup)
    return resolve_sociedad(sociedad,asociaciones)

def resolve_sociedad(sociedad,asociaciones):
    real_personas = set()
    sociedad = db_worker.find_or_create_sociedades([sociedad])[0]
    for rol,personas in asociaciones.iteritems():
        new_personas = [persona for persona in personas if persona not in real_personas]
        new_personas = set(db_worker.find_or_create_personas(new_personas))
        real_personas.update(new_personas)
        resolved_personas = [persona for persona in real_personas if persona in personas]
        db_worker.find_or_create_asociaciones(resolved_personas,sociedad,rol) #create associations
    print sociedad.nombre,len(sociedad.personas)
    return sociedad

def scrape_sociedad_personas(sociedad,html):
    directores = scrape_sociedad_directores(sociedad,html)
    subscriptores = scrape_sociedad_subscriptores(sociedad,html)
    dignatarios = scrape_sociedad_dignatarios(sociedad,html)
    return dict({'directores':directores, 'subscriptores':subscriptores}.items() +  {tupl[0]:[tupl[1]] for tupl in dignatarios}.items())

def scrape_sociedad_directores(sociedad,html):
    try:
        directores = [Persona(persona) for persona in parser.collect_directores(html)]
    except Exception as e:
        print e
        return []
    return directores

def scrape_sociedad_subscriptores(sociedad,html):
    try:
        subscriptores = [Persona(persona) for persona in parser.collect_subscriptores(html)]
    except Exception as e:
        print e
        return []
    return subscriptores

def scrape_sociedad_dignatarios(sociedad,html):
  try:
      dignatarios = parser.collect_dignatarios(html)
      dignatarios = [(pair[0],Persona(pair[1])) for pair in dignatarios]
  except Exception as e:
    return []
  return dignatarios

def scrape_sociedad_data(sociedad,html):
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
                print e
                sleep(0.1)

