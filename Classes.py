from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import deferred
from sqlalchemy import *
from sqlalchemy.types import Unicode, UnicodeText
from sqlalchemy.orm import relationship,backref
from datetime import datetime
import requests

from bs4 import BeautifulSoup,SoupStrainer

Base = declarative_base()

class Sociedad(Base):
  __tablename__ = 'sociedades'
  
  id = Column(Integer, Sequence('sociedad_id_seq'))
  nombre= Column(Unicode(200))
  ficha = Column(Integer(15), primary_key=True)
  capital = deferred(Column(Float(15)))
  moneda = deferred(Column(Unicode(50)))
  agente = deferred(Column(Unicode(200)))
  notaria = deferred(Column(Unicode(50)))
  fecha_registro = deferred(Column(Date))
  capital_text = deferred(Column(UnicodeText))
  representante_text = deferred(Column(UnicodeText))
  status = deferred(Column(Unicode(15)))
  duracion = deferred(Column(Unicode(15)))
  provincia = deferred(Column(Unicode(25)))
  visited = Column(Boolean)
  personas = relationship("Asociacion")

  def __init__(self,nombre,ficha):
    self.nombre = str(nombre)
    self.ficha = ficha
    self.visited = False
    self.html = None
    
  def __getitem__(self,key):
    return getattr(self, key)

  def __hash__(self):
    return hash(self.ficha)

  def __str__(self):
    return "Sociedad(%s)" % (self.nombre)

  def __repr__(self):
    return "Sociedad(%s)" % (self.ficha)

  def __eq__(self,other):
    return self.ficha == other.ficha
    
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

  def __hash__(self):
    return hash(self.socidedad_id, self.persona_id, self.rol)

  def __eq__(self,other):
    return (self.persona_id == other.persona_id and self.sociedad_id == other.persona_id and self.rol == other.rol)


class Persona(Base):
  __tablename__ = 'personas'
  
  id = Column(Integer, Sequence('persona_id_seq'))
  nombre = Column(Unicode(100), primary_key=True)
  sociedades = relationship(Asociacion)

  def __init__(self,nombre):
    self.nombre = nombre
    pass

  def __hash__(self):
    return hash(self.nombre)

  def __getitem__(self,key):
    return getattr(self, key)

  def __str__(self):
    return "Persona(%s)" % (self.nombre)

  def __repr__(self):
    return "Persona(%s)" % (self.nombre)

  def __eq__(self,other):
    return self.nombre == other.nombre
