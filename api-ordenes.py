from suds.client import Client

import config
import urlparse
import urllib
import os
import csv
import datetime
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


def get_date(dateFormat="%d-%m-%Y", backDays=0):
    # Este es el posta
    timeNow = datetime.datetime.now()
    #StartDate = "12/10/15"
    #timeNow = datetime.datetime.strptime(StartDate, "%m/%d/%y")
    if (backDays != 0):
        anotherTime = timeNow - datetime.timedelta(days=backDays)
    else:
        anotherTime = timeNow
    return anotherTime.strftime(dateFormat)

backDays = 1
output_format = '%Y%m%d'  # '%d-%m-%Y'
desde = get_date(output_format, backDays)
print "Desde: " + desde

backDays = 0
output_format = '%Y%m%d'  # '%d-%m-%Y'
hasta = get_date(output_format, backDays)
print "Hasta: " + hasta

tipos_ordenes = ["ACME", "CAME"]
modos = ["CREACION", "MODIFICACION"]

for tipo_orden in tipos_ordenes:
    for modo in modos:
        result = client.service.si_gobabierto(tipo_orden, desde, hasta, modo)
        print "llamando a la API con valores " + tipo_orden + " y " + modo + ""
        print "la API returneo con " + str(len(result)) + " resultados"
        for record in result:
            new_orden = generar_orden(record)

            session.add(Orden(**new_orden))
            session.commit()
