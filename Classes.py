from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import *
from sqlalchemy.types import Unicode, UnicodeText
from sqlalchemy.orm import relationship,backref
from datetime import datetime
from Sociedad import Sociedad
import requests
from itertools import izip
from bs4 import BeautifulSoup,SoupStrainer

Base = declarative_base()

class Sociedad(Base):
  __tablename__ = 'sociedades'
  
  id = Column(Integer, Sequence('sociedad_id_seq'), primary_key=True)
  nombre= Column(Unicode(50))
  ficha = Column(Integer(15))
  capital = Column(Float(15))
  moneda = Column(Unicode(50))
  notaria = Column(Unicode(50))
  fecha_registro = Column(Date)
  capital_text = Column(UnicodeText)
  representante_text = Column(UnicodeText)
  status = Column(Unicode(15))
  duracion = Column(Unicode(15))
  provincia = Column(Unicode(25))
  visited = Column(Boolean)
  personas = relationship("Asociacion")

  def get_ficha_html(self):
    url = 'https://www.registro-publico.gob.pa/scripts/nwwisapi.dll/conweb/MESAMENU?TODO=SHOW&ID=%s' % str(self.ficha)
    return BeautifulSoup(requests.get(url).text, 'html.parser',parse_only=SoupStrainer('table'))

  def __init__(self,nombre,ficha):
    self.nombre = nombre
    self.ficha = ficha
    self.visited = False
    
  def __getitem__(self,key):
    return getattr(self, key)

  def __str__(self):
    return "Sociedad(%s)" % (self.ficha)

    
class Asociacion(Base):
  __tablename__ = 'asociaciones'
  
  persona_id = Column(Integer, ForeignKey('personas.id'), primary_key=True)
  sociedad_id = Column(Integer, ForeignKey('sociedades.id'), primary_key=True)
  rol = Column(String(20), primary_key=True)
  sociedad = relationship(Sociedad)
  persona = relationship("Persona")

  def __init__(self,persona_id,sociedad_id,rol):
    self.persona_id = persona_id
    self.sociedad_id = sociedad_id
    self.rol = rol
    
  def __getitem__(self,key):
    return getattr(self, key)

  def __str__(self):
    return "Asociacion(%s : %s)" % (self.persona,self.sociedad)


class Persona(Base):
  __tablename__ = 'personas'
  
  id = Column(Integer, Sequence('persona_id_seq'), primary_key=True)
  nombre = Column(Unicode(100))
  sociedades = relationship(Asociacion)

  def __init__(self,nombre):
    self.nombre = nombre
    pass
    
  def __getitem__(self,key):
    return getattr(self, key)

  def __str__(self):
    return "Persona(%s)" % (self.nombre)
