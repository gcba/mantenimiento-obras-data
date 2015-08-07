from suds.client import Client
import csv

import urlparse, urllib, os

url = urlparse.urljoin('file:', urllib.pathname2url(os.path.abspath('I016 - Ordenes QA.WSDL')))
client = Client(url, username='GABIERTO', password="gab2015!!")


desde = "20140801"
hasta = "20150807"
tipos_ordenes = ["ACRE", "CARE"]
modo = "CREACION"
columnas = ["NRO_ORDEN", "CLASE_ORDEN", "ORDEN_SUPERIOR", "ORDEN_INFERIOR", "DESCRIPCION", "UBIC_TECNICA_DESC", "STATUS_USUARIO", "GRP_PLANIFICADOR", "PTO_RESPONSABLE", "PRIORIDAD", "FECHA_CREACION", "FECHA_INI_EXTREMO", "FECHA_FIN_EXTREMO", "UBIC_TECNICA", "CALLE", "ALTURA", "AREA_EMPRESA", "EMP_OBJ_MANTENIM", "LOCAL", "CAMPO_CLASIF", "FECHA_ULT_MODIF", "RESP_ULT_MODIF", "CLAVE_MODELO", "CLAVE_MODELO_TXT", "DURACION", "STATUS_OPERACION"]

nombre_archivo = "ordenes-" + tipos_ordenes[0]+"-"+desde + "-" + hasta + ".csv" 

file_ordenes = open(nombre_archivo, 'w')
file_ordenes.seek(0)

csv_writer = csv.writer(file_ordenes, delimiter=';')
csv_writer.writerow(columnas)

for tipo_orden in tipos_ordenes:
	result = client.service.si_gobabierto(tipo_orden, desde, hasta, modo)
	print result
	for record in result:
		record_values = []
		for columna in columnas:
			if record[columna] is not None:
				record_value = record[columna].encode('utf8','ignore')
			else:
				record_value = ""
			record_values.append(record_value)
		csv_writer.writerow(record_values)