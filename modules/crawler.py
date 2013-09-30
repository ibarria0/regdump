import requests
import Classes
from modules import parser,db_worker
from bs4 import BeautifulSoup,SoupStrainer

only_row_tags = SoupStrainer("tr")
a_table = SoupStrainer("table")
consulta = SoupStrainer(id="CONSULTA")

def query_url(page,query):
  return ('https://www.registro-publico.gob.pa/scripts/nwwisapi.dll/conweb/MESAMENU?TODO=MER4&START=%s&FROM=%s' % (str(page),query))

def parse_query_result(html):
  return [Classes.Sociedad(row.td.string, row.find('a').string) for row in BeautifulSoup(html,"html.parser").find_all(a_table)[6].contents[1:]]

def collect_query(query):
  page = 1
  results = []
  while True:
    html = requests.get(query_url(page,query)).text
    results.extend(parse_query_result(html))
    if len(results) < (15 * (page/15)):
      break
    else:
      page = page + 15
  return db_worker.find_or_create_sociedades(results)

def scrape_sociedad(sociedad):
  html = sociedad.get_ficha_html()
  scrape_sociedad_data(sociedad,html)
  scrape_sociedad_personas(sociedad,html)
  return sociedad

def scrape_sociedad_personas(sociedad,html):
  try:
    directores = scrape_sociedad_directores(sociedad,html)
    subscriptores = scrape_sociedad_subscriptores(sociedad,html)
    dignatarios = scrape_sociedad_dignatarios(sociedad,html)
    agente = scrape_sociedad_agente(sociedad,html)
  except:
    return set()
  return set(directores + subscriptores + dignatarios + agente) 

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
    sociedad.moneda = parser.collect_moneda(html)
    sociedad.capital = parser.collect_capital(html)
    sociedad.capital_text = parser.collect_capital_text(html)
    sociedad.representante_text = parser.collect_representante_text(html)
  except:
    return sociedad
  return sociedad
