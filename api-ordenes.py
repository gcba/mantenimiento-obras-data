from suds.client import Client

import config
import urlparse
import urllib
import os
import json
import urllib2
import csv
from data_models import *

from sqlalchemy.orm import sessionmaker

engine = create_engine(config.db_url)
# create a configured "Session" class
Session = sessionmaker(bind=engine)
# create a Session
session = Session()

load_data()

url = urlparse.urljoin(
    'file:', urllib.pathname2url(config.wsdl_path))
client = Client(
    url, username=config.wsdl_username, password=config.wsdl_password)

desde = "20151103"
hasta = "20151104"
tipos_ordenes = ["ACME", "CAME"]
modos = ["CREACION", "MODIFICACION"]
columnas = ["NRO_ORDEN", "CLASE_ORDEN", "DESCRIPCION", "UBIC_TECNICA", "UBIC_TECNICA_DESC", "FECHA_CREACION", "FECHA_INI_EXTREMO",
            "FECHA_FIN_EXTREMO",  "CALLE", "ALTURA", "CLAVE_MODELO", "CLAVE_MODELO_TXT", "AREA_EMPRESA", "STATUS_USUARIO", "FECHA_ULT_MODIF"]

for tipo_orden in tipos_ordenes:
    for modo in modos:
        result = client.service.si_gobabierto(tipo_orden, desde, hasta, modo)
        print "llamando a la API con valores " + tipo_orden + " y " + modo + ""
        print "la API returneo con " + str(len(result)) + " resultados"
        for record in result:
            new_orden = {}
            for columna in columnas:
                if record[columna] is not None:
                    new_orden[columna.lower()] = record[columna].encode(
                        'utf8', 'ignore')
                else:
                    new_orden[columna.lower()] = None

            ev = session.query(Orden).filter(Orden.nro_orden == new_orden[
                'nro_orden'], Orden.clave_modelo == new_orden['clave_modelo']).count()
            if not ev:
                # Si no existe el record en la DB, calcular geocodificacion,
                # comuna, y agregar a la DB

                # Mapear clave_modelo al tipo de obra definido
                new_orden["tipo_obra_id"] = None
                if new_orden["clave_modelo"] is not None:
                    result = session.query(ClaveTipo).filter(
                        ClaveTipo.clave == new_orden["clave_modelo"]).first()
                    if result:
                        new_orden["tipo_obra_id"] = result.tipo_id

                # Mapear status_usuario al status humano definido
                new_orden["status_id"] = None
                if new_orden["status_usuario"] is not None:
                    result = session.query(StatusTraduccion).filter(
                        StatusTraduccion.status_usuario == new_orden["status_usuario"]).first()
                    if result:
                        new_orden["status_id"] = result.status_id

                direccion_list = new_orden[
                    "ubic_tecnica_desc"].split("-")[0].split(" ")
                altura = direccion_list[-1]
                calle = " ".join(
                    direccion_list[:-1]).replace(",", "").replace("PARCELA", "").replace(" ", "%20")

                geocod = json.load(urllib2.urlopen(
                    'http://ws.usig.buenosaires.gob.ar/rest/normalizar_y_geocodificar_direcciones?calle=' + calle + '&altura=' + altura + '&desambiguar=1'))
                if "Normalizacion" in geocod.keys():
                    new_orden["tipo_resultado"] = geocod[
                        "Normalizacion"]["TipoResultado"]
                if "GeoCodificacion" in geocod.keys():
                    if type(geocod["GeoCodificacion"]) is dict:
                        new_orden["geo_x"] = geocod["GeoCodificacion"]["x"]
                        new_orden["geo_y"] = geocod["GeoCodificacion"]["y"]
                    else:
                        new_orden["tipo_resultado"] = geocod["GeoCodificacion"]

                comuna = "NA"
                try:
                    res_comuna = json.load(urllib2.urlopen(
                        'http://ws.usig.buenosaires.gob.ar/datos_utiles?calle=' + calle + '&altura=' + altura))
                    comuna = res_comuna["comuna"].split(" ")[1]
                except ValueError as e:
                    comuna = "Error direccion"
                except:
                    comuna = "Error OTRO"
                new_orden["comuna"] = comuna

                session.add(Orden(**new_orden))
                session.commit()
