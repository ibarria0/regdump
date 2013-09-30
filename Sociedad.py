from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import *
from sqlalchemy.types import Unicode, UnicodeText
from sqlalchemy.orm import relationship,backref
from datetime import datetime

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
  provincia = Column(Unicode(15))

  def __init__(self,nombre,ficha):
    self.nombre = nombre
    self.ficha = ficha
    
  def __getitem__(self,key):
    return getattr(self, key)

  def __str__(self):
    return "Sociedad(%s)" % (self.ficha)

