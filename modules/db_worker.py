from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import Classes
import os

db_url = os.environ['panadata_db']
engine = create_engine(db_url,  encoding='latin-1',echo=False)
session_maker = sessionmaker(bind=engine)
session = session_maker()
Classes.Base.metadata.create_all(engine)

def find_or_create_sociedades(sociedades):
  result = []
  for sociedad in sociedades:
    instance = session.query(Classes.Sociedad).filter(Classes.Sociedad.ficha==sociedad.ficha).first()
    if instance: 
      sociedad.id = instance.id
      session.merge(sociedad)
      result.append(instance)
    else:
      session.add(sociedad)
      result.append(sociedad)
  session.commit()
  return result

def find_by_fichas(fichas):
    return session.query(Classes.Sociedad).filter(Classes.Sociedad.ficha.in_(fichas)).all()

def get_fichas():
    try:
        return set(zip(*session.query(Classes.Sociedad.ficha).all())[0])
    except:
        return set()

def mark_sociedades_visited(sociedades):
  session.query(Classes.Sociedad).filter(Classes.Sociedad.id.in_([sociedad.id for sociedad in sociedades])).update({'visited':True},synchronize_session='fetch')
  session.commit()
  return sociedades

def find_or_create_personas(personas):
  result = []
  for persona in personas:
    instance = session.query(Classes.Persona).filter(Classes.Persona.nombre==persona.nombre).first()
    if instance: 
      result.append(instance)
    else:
      session.add(persona)
      result.append(persona)
  session.commit()
  return result

def find_or_create_asociaciones(personas,sociedad,rol):
  result = []
  for persona in find_or_create_personas(personas):
    instance = session.query(Classes.Asociacion).filter(Classes.Asociacion.persona_id==persona.id).filter(Classes.Asociacion.sociedad_id==sociedad.id).filter(Classes.Asociacion.rol==rol.upper()).first()
    if instance: 
      result.append(instance)
    else:
      asociacion = Classes.Asociacion(persona.id,sociedad.id,unicode(rol.upper()))
      session.add(asociacion)
      result.append(asociacion)
  session.commit()
  return result
