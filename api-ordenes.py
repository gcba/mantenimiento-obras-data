from suds.client import Client
import csv

import config
import urlparse
import urllib
import os

from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

conn_str = "mysql://" + config.db["user"] + ":" + \
    config.db["password"] + "@" + config.db["host"] + "/" + config.db["db"]
engine = create_engine(conn_str)
# create a configured "Session" class
Session = sessionmaker(bind=engine)
# create a Session
session = Session()
Base = declarative_base()


class Orden(Base):
    __tablename__ = 'ordenes'
    orden_id = Column(Integer, primary_key=True, autoincrement='ignore_fk')
    nro_orden = Column(String(50))
    clase_orden = Column(String(100))
    descripcion = Column(String(100))
    ubic_tecnica = Column(String(100))
    ubic_tecnica_desc = Column(String(100))
    fecha_creacion = Column(Date)
    fecha_ini_extremo = Column(Date)
    fecha_fin_extremo = Column(Date)
    calle = Column(String(100))
    altura = Column(String(10))
    clave_modelo = Column(String(50))
    clave_modelo_txt = Column(String(100))
    area_empresa = Column(String(10))
    status_usuario = Column(String(50))
    fecha_ult_modif = Column(Date)

    def __repr__(self):
        return "<Orden(nro_orden='%s', clase_orden='%s', descripcion='%s', ubic_tecnica='%s', ubic_tecnica_desc='%s', fecha_creacion='%s', fecha_ini_extremo='%s', fecha_fin_extremo='%s', calle='%s', altura='%s', clave_modelo_txt='%s', status_operacion='%s')>" % (self.nro_orden, self.clase_orden, self.descripcion, self.ubic_tecnica, self.ubic_tecnica_desc, self.fecha_creacion,
                                                                                                                                                                                                                                                                       self.fecha_ini_extremo, self.fecha_fin_extremo, self.calle, self.altura, self.clave_modelo_txt, self.status_operacion)

Base.metadata.create_all(engine, checkfirst=True)

url = urlparse.urljoin(
    'file:', urllib.pathname2url(os.path.abspath('I016 - Ordenes PRD.WSDL')))
client = Client(url, username='GABIERTO', password="2x5=diez")  # PROD
# QA:
#     client = Client(url, username='GABIERTO', password="gab2015!!")

desde = "20150801"
hasta = "20150807"
tipos_ordenes = ["ACRE", "CARE"]
modo = "CREACION"
columnas = ["NRO_ORDEN", "CLASE_ORDEN", "DESCRIPCION", "UBIC_TECNICA", "UBIC_TECNICA_DESC", "FECHA_CREACION", "FECHA_INI_EXTREMO",
            "FECHA_FIN_EXTREMO",  "CALLE", "ALTURA", "CLAVE_MODELO", "CLAVE_MODELO_TXT", "AREA_EMPRESA", "STATUS_USUARIO", "FECHA_ULT_MODIF"]

nombre_archivo = "ordenes-" + desde + "-" + hasta + ".csv"

file_ordenes = open(nombre_archivo, 'w')
file_ordenes.seek(0)

for tipo_orden in tipos_ordenes:
    result = client.service.si_gobabierto(tipo_orden, desde, hasta, modo)
    print result
    for record in result:
        new_orden = {}
        for columna in columnas:
            if record[columna] is not None:
                new_orden[columna.lower()] = record[columna].encode(
                    'utf8', 'ignore')
            else:
                new_orden[columna.lower()] = None
        # ev = session.query(Orden).filter(Orden.nro_orden == new_orden[
        #     'nro_orden'], Orden.clave_modelo == new_orden['clave_modelo']).count()
        # if not ev:
        # Add record to DB
        session.add(Orden(**new_orden))
        session.commit()
