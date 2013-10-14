from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import Classes
import os

db_url = os.environ['panadata_db']
engine = create_engine(db_url,  encoding='latin-1',echo=False)
session_maker = sessionmaker(bind=engine)
session = session_maker()
Classes.Base.metadata.create_all(engine)

def rollback():
    session.rollback()

def find_or_create_sociedades(sociedades):
    if len(sociedades) > 0:
        query = session.query(Classes.Sociedad).filter(Classes.Sociedad.ficha.in_([sociedad.ficha for sociedad in sociedades]))
        result = query.merge_result(sociedades)
        session.commit()
        return list(result)
    else:
        return []

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
    if len(personas) > 0:
        nombres = [persona.nombre for persona in personas]
        query = session.query(Classes.Persona).filter(Classes.Persona.nombre.in_(nombres))
        result = query.merge_result(personas)
        session.commit()
        return list(result)
    else:
        return []

def find_or_create_asociaciones(personas,sociedad,rol):
    if len(personas) > 0:
        persona_ids = [persona.id for persona in personas]
        query = session.query(Classes.Asociacion).filter(Classes.Asociacion.sociedad_id==sociedad.id)
        asociaciones = [Classes.Asociacion(persona.id,sociedad.id,unicode(rol.upper())) for persona in personas]
        result = query.merge_result(asociaciones)
        session.commit()
        return list(result)
    else:
        return []
