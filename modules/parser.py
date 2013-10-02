import re
from datetime import datetime
from bs4 import BeautifulSoup

def collect_dignatarios(soup):
  cells = [unicode(cell.string) for cell in soup.find('td',text='Nombre del Dignatario').parent.parent.find_next_sibling('table').find_all('td') if cell.string != None]
  return zip(cells[0::2],cells[1::2])

def collect_representante_text(soup):
  cells = soup.find('td',text='Capital').parent.parent.find_next_sibling('table').find_next_sibling('table').find_next_sibling('table').find_all('td')
  return ''.join([unicode(cell.string) for cell in cells if cell.string != None])

def collect_capital_text(soup):
  return ''.join([unicode(cell.string) for cell in soup.find('td',text='Capital').parent.parent.find_next_sibling('table').find_all('td') if cell.string != None])

def collect_directores(soup):
  return [unicode(row.td.string) for row in soup.find('td',text='Nombre de los Directores').parent.parent.find_next_sibling('table').find_all('tr') if row.td.string != None]

def collect_subscriptores(soup):
  return [unicode(row.td.string) for row in soup.find('td',text='Nombre de los Suscriptores').parent.parent.find_next_sibling('table').find_all('tr') if row.td.string != None]

def collect_moneda(soup):
  return unicode(soup.find('td',text='Moneda: ').find_next_sibling('td').string)

def collect_capital(soup):
  return float(re.sub(r'[^\d.]', '',soup.find('td',text='Monto de Capital:').find_next_sibling('td').string))

def collect_notaria(soup):
  return unicode(soup.find('td',text='Notaria:').find_next_sibling('td').find_next_sibling('td').string)

def collect_fecha_registro(soup):
  fecha = soup.find('td',text='Fecha de Registro:').find_next_sibling('td').string
  return datetime.strptime(fecha, "%d-%m-%Y" ).date()

def collect_status(soup):
  return unicode(soup.find('td',text='Status:').find_next_sibling('td').string)

def collect_provincia(soup):
  return unicode(soup.find('td',text='Provincia Notaria:').find_next_sibling('td').string)

def collect_duracion(soup):
  return unicode(soup.find('td',text='Domicilio:').find_previous_sibling('td').string)

def collect_agente(soup):
  return unicode(soup.find('td',text='Agente Residente:').find_next_sibling('td').string)
