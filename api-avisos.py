from suds.client import Client
import csv

url = "file:///Users/dgiga/Documents/Python Scripts/deloitte/I016_AVISOS_GOB_ABIERTO_QA.WSDL"
client = Client(url, username='GABIERTO', password="gab2015!!")

desde = "20150608"
hasta = "20150614"
modo = "CREACION"

columnas = ["NRO_AVISO","CLASE_AVISO","TEXTO_EXPLICATIVO","STATUS_USUARIO","GRP_PLANIFICADOR","PRO_RESPONSABLE","PRIORIDAD","FECHA_CREACION","FECHA_AVISO","HORA_AVISO","FECHA_INI_DESEADO","HORA_INI_DESEADO","FECHA_FIN_DESEADO","HORA_FIN_DESEADO","UBIC_TECNICA","CALLE","ALTURA","AREA_EMPRESA","EMP_OBJ_MANTENIM","LOCAL","CAMPO_CLASIF","FECHA_ULT_MODIF","GRP_PRESTACIONES","COD_PRESTACIONES","TXT_CODIGO","RESP_ULT_MODIF","NRO_ORDEN"]
tipos_aviso = ["AP", "EM", "SU"]
# tipos_aviso = ["DI","OF","OP","RE","SU"]

nombre_archivo = "avisos-" + desde + "-" + hasta + ".csv"

file_avisos = open(nombre_archivo, 'w')
file_avisos.seek(0)

csv_writer = csv.writer(file_avisos, delimiter=';')
csv_writer.writerow(columnas)

for tipo_aviso in tipos_aviso:
	result = client.service.si_gob_abierto(tipo_aviso, desde, hasta,modo)
	for record in result:
		record_values = []
		for columna in columnas:
			if record[columna] is not None:
				record_value = record[columna].encode('utf8','ignore')
			else:
				record_value = ""
			record_values.append(record_value)
		csv_writer.writerow(record_values)