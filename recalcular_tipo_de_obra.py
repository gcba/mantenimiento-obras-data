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


id_default = session.query(TipoObra).filter(
    TipoObra.name == 'Otros').first().tipo_id

clave_to_id = {}
for clave_tipo in session.query(ClaveTipo):
    clave_to_id[clave_tipo.clave] = clave_tipo.tipo_id

for row in session.query(Orden).all():
    row.tipo_obra_id = clave_to_id.get(row.clave_modelo, id_default)
session.commit()
