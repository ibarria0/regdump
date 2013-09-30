from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import Classes
import os

db_url = 'postgresql+psycopg2://' + os.environ['PANAUSER'] + ':' + os.environ['PANAPASS'] + '@localhost/panadata'
engine = create_engine(db_url,  encoding='latin-1',echo=False)
session_maker = sessionmaker(bind=engine)
session = session_maker()
Classes.Base.metadata.create_all(engine)

def find_or_create_sociedades(sociedades):
  result = []
  for sociedad in sociedades:
    instance = session.query(Classes.Sociedad).filter(Classes.Sociedad.ficha==sociedad.ficha).first()
    if instance: 
      result.append(instance)
    else:
      session.add(sociedad)
      session.commit()
      result.append(sociedad)
  return result


def find_or_create_personas(personas):
  result = []
  for persona in personas:
    instance = session.query(Classes.Persona).filter(Classes.Persona.nombre==persona.nombre).first()
    if instance: 
      result.append(instance)
    else:
      session.add(persona)
      session.commit()
      result.append(persona)
  return result

def find_or_create_asociaciones(personas,sociedad,rol):
  result = []
  for persona in find_or_create_personas(personas):
    instance = session.query(Classes.Asociacion).filter(Classes.Asociacion.persona_id==persona.id).filter(Classes.Asociacion.sociedad_id==sociedad.id).filter(Classes.Asociacion.rol==rol.upper()).first()
    if instance: 
      result.append(instance)
    else:
      asociacion = Classes.Asociacion(persona.id,sociedad.id,rol.upper())
      session.add(asociacion)
      session.commit()
      result.append(asociacion)
  return result
