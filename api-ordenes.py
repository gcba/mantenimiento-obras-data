from suds.client import Client
import csv

import config
import urlparse
import urllib
import os
import pdb

from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

engine = create_engine(config.db_url)
# create a configured "Session" class
Session = sessionmaker(bind=engine)
# create a Session
session = Session()
Base = declarative_base()


class Orden(Base):
    __tablename__ = 'ordenes'
    nro_orden = Column(String(50), primary_key=True)
    clase_orden = Column(String(100))
    descripcion = Column(String(100))
    ubic_tecnica = Column(String(100))
    ubic_tecnica_desc = Column(String(100))
    fecha_creacion = Column(Date)
    fecha_ini_extremo = Column(Date, nullable=True)
    fecha_fin_extremo = Column(Date, nullable=True)
    calle = Column(String(50))
    altura = Column(String(10))
    clave_modelo_txt = Column(String(50))
    status_operacion = Column(String(50))

    def __repr__(self):
        return "<Orden(nro_orden='%s', clase_orden='%s', descripcion='%s', ubic_tecnica='%s', ubic_tecnica_desc='%s', fecha_creacion='%s', fecha_ini_extremo='%s', fecha_fin_extremo='%s', calle='%s', altura='%s', clave_modelo_txt='%s', status_operacion='%s')>" % (self.nro_orden, self.clase_orden, self.descripcion, self.ubic_tecnica, self.ubic_tecnica_desc, self.fecha_creacion,
                                                                                                                                                                                                                                                                       self.fecha_ini_extremo, self.fecha_fin_extremo, self.calle, self.altura, self.clave_modelo_txt, self.status_operacion)

Base.metadata.create_all(engine, checkfirst=True)

url = urlparse.urljoin(
    'file:', urllib.pathname2url(config.wsdl_path))
client = Client(
    url, username=config.wsdl_username, password=config.wsdl_password)

desde = "20150801"
hasta = "20150807"
tipos_ordenes = ["ACRE", "CARE"]
modo = "CREACION"
columnas = ["NRO_ORDEN", "CLASE_ORDEN", "DESCRIPCION", "UBIC_TECNICA", "UBIC_TECNICA_DESC", "FECHA_CREACION", "FECHA_INI_EXTREMO",
            "FECHA_FIN_EXTREMO",  "CALLE", "ALTURA", "CLAVE_MODELO_TXT", "STATUS_OPERACION"]

nombre_archivo = "ordenes-" + desde + "-" + hasta + ".csv"

file_ordenes = open(nombre_archivo, 'w')
file_ordenes.seek(0)

csv_writer = csv.writer(file_ordenes, delimiter=';')
csv_writer.writerow(columnas)

for tipo_orden in tipos_ordenes:
    result = client.service.si_gobabierto(tipo_orden, desde, hasta, modo)
    print result
    for record in result:
        new_orden = {}
        record_values = []
        for columna in columnas:
            if record[columna] is not None:
                new_orden[columna.lower()] = record[columna].encode(
                    'utf8', 'ignore')
                record_value = record[columna].encode('utf8', 'ignore')
            else:
                new_orden[columna.lower()] = ""
                record_value = ""
            record_values.append(record_value)

        if (new_orden['fecha_fin_extremo'] == ''):
            new_orden['fecha_fin_extremo'] = None

        # Add record to DB
        session.merge(Orden(**new_orden))

    session.commit()
    # Write record to CSV
    csv_writer.writerow(record_values)
