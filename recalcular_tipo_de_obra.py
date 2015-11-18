import csv
import config

from sqlalchemy.orm import sessionmaker

from data_models import *

engine = create_engine(config.db_url)
# create a configured "Session" class
Session = sessionmaker(bind=engine)
# create a Session
session = Session()
Base = declarative_base()


for row in session.query(Orden):

    tipo = session.query(ClaveTipo).filter(
        ClaveTipo.clave == row.clave_modelo).first()

    if tipo:
        tipo_id = tipo.tipo_id
    else:
        tipo_id = session.query(TipoObra).filter(
            TipoObra.name == 'Otros').first().tipo_id

    row.tipo_id = tipo_id
    session.commit()
