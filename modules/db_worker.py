from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import Classes
import os

db_url = os.environ['panadata_db']
engine = create_engine(db_url,  encoding='latin-1',echo=False)
session_maker = sessionmaker(bind=engine)
Classes.Base.metadata.create_all(engine)

def resolve_sociedad(sociedad,personas,asociaciones):
    session = session_maker()
    while True:
        try:
            sociedad = find_or_create_sociedades([sociedad],session)[0]
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
    session.close()
    return sociedad

def find_or_create_sociedades(sociedades,session):
    if len(sociedades) > 0:
        query = session.query(Classes.Sociedad).filter(Classes.Sociedad.ficha.in_([sociedad.ficha for sociedad in sociedades]))
        result = query.merge_result(sociedades)
        session.commit()
        return list(query.all())
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
        result = query.merge_result(personas)
        session.commit()
        return set(query.all())
    else:
        return set()

def find_or_create_asociaciones(personas,sociedad,rol,session):
    if len(personas) > 0:
        persona_ids = [persona.id for persona in personas]
        query = session.query(Classes.Asociacion).filter(Classes.Asociacion.sociedad_id==sociedad.id)
        asociaciones = [Classes.Asociacion(persona.id,sociedad.id,str(rol.upper())) for persona in personas]
        result = query.merge_result(asociaciones)
        session.commit()
        return list(result)
    else:
        return []
