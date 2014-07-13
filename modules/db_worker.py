from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import Classes
import os
from time import sleep
import logging

db_url = os.environ['panadata_db']
engine = create_engine(db_url, convert_unicode=True, encoding='latin-1',echo=False)
session_maker = sessionmaker(bind=engine)
Classes.Base.metadata.create_all(engine)
logger = logging.getLogger('db_worker')

def resolve_asociaciones(personas,asociaciones,session):
    personas = list(personas)
    for key in asociaciones.keys():
        tmp = set()
        for persona in asociaciones[key]:
            tmp.add(personas[personas.index(persona)])
        asociaciones[key] = tmp
    return asociaciones

def resolve_sociedad(sociedad,personas,asociaciones):
    session = session_maker()
    while True:
        try:
            sociedad = find_or_create_sociedad(sociedad,session)
            personas = find_or_create_personas(personas,session)
            asociaciones = resolve_asociaciones(personas,asociaciones,session)
            for rol in asociaciones.keys():
                find_or_create_asociaciones(asociaciones[rol],sociedad,rol,session) #create associations
            logger.info('sociedad %s resolved', sociedad.nombre)
            break
        except Exception as e:
            logger.error(e)
            session.rollback()
            session.expunge_all()
            continue
    session.expunge_all()
    session.close()
    return sociedad

def find_or_create_sociedad(sociedad,session):
    instance = session.query(Classes.Sociedad).filter(Classes.Sociedad.ficha == sociedad.ficha).first()
    if instance:
       return instance
    else:
        session.add(sociedad)
        session.commit()
        return sociedad


def find_or_create_sociedades(sociedades,session):
    if len(sociedades) > 0:
        query = session.query(Classes.Sociedad).filter(Classes.Sociedad.ficha.in_([sociedad.ficha for sociedad in sociedades]))
        result = list(query.merge_result(sociedades))
        session.commit()
        return result
    else:
        return []

def find_by_fichas(fichas):
    session = session_maker()
    return session.query(Classes.Sociedad).filter(Classes.Sociedad.ficha.in_(fichas)).all()

def find_max_ficha():
    session = session_maker()
    return session.query(Classes.Sociedad.ficha).order_by(Classes.Sociedad.ficha.desc()).first()[0]

def get_fichas():
    session = session_maker()
    try:
        return {sociedad.ficha for sociedad in session.query(Classes.Sociedad).all()}
    except Exception as e:
        print(e)
        return set()

def get_personas():
    session = session_maker()
    try:
        return {persona.nombre:persona for persona in session.query(Classes.Persona).all()}
    except:
        return set()

def find_or_create_personas(personas,session):
    if len(personas) > 0:
        nombres = [persona.nombre for persona in personas]
        query = session.query(Classes.Persona).filter(Classes.Persona.nombre.in_(nombres))
        result = list(query.merge_result(personas))
        session.commit()
        return set(result)
    else:
        return set()

def find_or_create_asociaciones(personas,sociedad,rol,session):
    if len(personas) > 0:
        persona_ids = [persona.nombre for persona in personas]
        query = session.query(Classes.Asociacion).filter(Classes.Asociacion.sociedad_id==sociedad.ficha)
        asociaciones = [Classes.Asociacion(persona.nombre,sociedad.ficha,str(rol.upper())) for persona in personas]
        result = list(query.merge_result(asociaciones))
        session.commit()
        return result
    else:
        return []

def get_sociedades():
    session = session_maker()
    sociedades = set()
    try:
        sociedades= {sociedad.nombre for sociedad in session.query(Classes.Sociedad).all()}
    except Exception as e:
        print(e)
    finally:
        session.expunge_all()
        session.close()
        return sociedades

