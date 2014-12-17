from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import deferred
from sqlalchemy import *
from sqlalchemy.types import Unicode, UnicodeText
from sqlalchemy.orm import relationship,backref
from datetime import datetime
import requests

from bs4 import BeautifulSoup,SoupStrainer

Base = declarative_base()

class Fundacion(Base):
  __tablename__ = 'fundaciones'

  nombre= Column(Unicode, unique=True)
  ficha = Column(Integer, primary_key=True)
  documento = Column(Integer)
  escritura = Column(Integer)
  patrimonio = deferred(Column(Float))
  moneda = deferred(Column(Unicode))
  agente = deferred(Column(Unicode))
  notaria = deferred(Column(Unicode))
  fecha_registro = deferred(Column(Date))
  patrimonio_text = deferred(Column(UnicodeText))
  firmante_text = deferred(Column(UnicodeText))
  status = deferred(Column(Unicode))
  duracion = deferred(Column(Unicode))
  provincia = deferred(Column(Unicode))
  personas = relationship("FundacionPersonas")
  html = deferred(Column(UnicodeText))
  created_at = Column(DateTime, default=datetime.now)
  updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

  def __init__(self,nombre,ficha):
    self.nombre = str(nombre)
    self.ficha = ficha
    self.html = None

  def __getitem__(self,key):
    return getattr(self, key)

  def __hash__(self):
    return hash(self.ficha)

  def __str__(self):
    return "Fundacion(%s)" % (self.nombre)

  def __repr__(self):
    return "Fundacion(%s)" % (self.ficha)

  def __eq__(self,other):
    return self.ficha == other.ficha

class FundacionPersonas(Base):
  __tablename__ = 'fundacion_personas'

  persona_id = Column(Integer , ForeignKey('personas.id'), primary_key=True)
  fundacion_id = Column(Integer, ForeignKey('fundaciones.ficha'), primary_key=True)
  rol = Column(String, primary_key=True)
  fundacion = relationship(Fundacion)
  persona = relationship("Persona")

  def __init__(self,persona_id,fundacion_id,rol):
    self.persona_id = persona_id
    self.fundacion_id = fundacion_id
    self.rol = rol

  def __getitem__(self,key):
    return getattr(self, key)

  def __str__(self):
    return "FundacionPersonas(%s : %s)" % (self.persona,self.fundacion)

  def __hash__(self):
    return hash(self.fundacion_id, self.persona_id, self.rol)

  def __eq__(self,other):
    return (self.persona_id == other.persona_id and self.fundacion_id == other.persona_id and self.rol == other.rol)

class Sociedad(Base):
  __tablename__ = 'sociedades'

  nombre= Column(Unicode, unique=True)
  ficha = Column(Integer, primary_key=True)
  capital = deferred(Column(Float))
  moneda = deferred(Column(Unicode))
  agente = deferred(Column(Unicode))
  notaria = deferred(Column(Unicode))
  fecha_registro = deferred(Column(Date))
  capital_text = deferred(Column(UnicodeText))
  representante_text = deferred(Column(UnicodeText))
  status = deferred(Column(Unicode))
  duracion = deferred(Column(Unicode))
  provincia = deferred(Column(Unicode))
  visited = Column(Boolean)
  personas = relationship("Asociacion")
  html = deferred(Column(UnicodeText))
  created_at = Column(DateTime, default=datetime.now)
  updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

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

  persona_id = Column(Integer , ForeignKey('personas.id'), primary_key=True)
  sociedad_id = Column(Integer, ForeignKey('sociedades.ficha'), primary_key=True)
  rol = Column(String, primary_key=True)
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
    return hash(self.sociedad_id, self.persona_id, self.rol)

  def __eq__(self,other):
    return (self.persona_id == other.persona_id and self.sociedad_id == other.persona_id and self.rol == other.rol)


class Persona(Base):
  __tablename__ = 'personas'

  id = Column(Integer, Sequence('persona_id_seq'), primary_key=True)
  nombre = Column(Unicode, unique=True)
  sociedades = relationship(Asociacion)
  created_at = Column(DateTime, default=datetime.now)
  updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


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
