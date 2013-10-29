import re
from datetime import datetime
from bs4 import BeautifulSoup

def exists(soup):
    try:
        nombre = soup.find(text="Nombre de la Sociedad:").parent.parent.parent.parent.find_next('td').string
        if nombre: return True
    except:
        return False

def collect_dignatarios(soup):
    dignatarios = {}
    cells = [str(cell.string) for cell in soup.find(text='Nombre del Dignatario').find_parent('table').find_next_sibling('table').find_all('td') if cell.string != None]
    for asoc in list(zip(cells[0::2],cells[1::2])):
        try:
            dignatarios[asoc[0]].append(asoc[1])
        except KeyError:
            dignatarios[asoc[0]] = [asoc[1]]
    return dignatarios

def collect_representante_text(soup):
  cells = soup.find('td',text='Capital').parent.parent.find_next_sibling('table').find_next_sibling('table').find_next_sibling('table').find_all('td')
  return ''.join([str(cell.string) for cell in cells if cell.string != None])

def collect_capital_text(soup):
  return ''.join([str(cell.string) for cell in soup.find('td',text='Capital').parent.parent.find_next_sibling('table').find_all('td') if cell.string != None])

def collect_directores(soup):
  return [str(row.td.string) for row in soup.find('td',text='Nombre de los Directores').parent.parent.find_next_sibling('table').find_all('tr') if row.td.string != None]

def collect_subscriptores(soup):
  return [str(row.td.string) for row in soup.find('td',text='Nombre de los Suscriptores').parent.parent.find_next_sibling('table').find_all('tr') if row.td.string != None]

def collect_moneda(soup):
  return str(soup.find('td',text='Moneda: ').find_next_sibling('td').string)

def collect_ficha(soup):
  return int(soup.find(text='No. Documento:').parent.parent.parent.find_previous_sibling().p.string)

def collect_nombre(soup):
  return str(soup.find(text="Nombre de la Sociedad:").parent.parent.parent.parent.find_next('td').string)

def collect_capital(soup):
  return float(re.sub(r'[^\d.]', '',soup.find('td',text='Monto de Capital:').find_next_sibling('td').string))

def collect_notaria(soup):
  return str(soup.find('td',text='Notaria:').find_next_sibling('td').find_next_sibling('td').string)

def collect_fecha_registro(soup):
  fecha = soup.find('td',text='Fecha de Registro:').find_next_sibling('td').string
  try:
    return datetime.strptime(fecha, "%d-%m-%Y" ).date()
  except:
    return None

def collect_status(soup):
  return str(soup.find('td',text='Status:').find_next_sibling('td').string)

def collect_provincia(soup):
  return str(soup.find('td',text='Provincia Notaria:').find_next_sibling('td').string)

def collect_duracion(soup):
  return str(soup.find('td',text='Domicilio:').find_previous_sibling('td').string)

def collect_agente(soup):
  return str(soup.find('td',text='Agente Residente:').find_next_sibling('td').string)
