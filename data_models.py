from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import config
import csv
import json
import urllib2

engine = create_engine(config.db_url)
# create a configured "Session" class
Session = sessionmaker(bind=engine)
# create a Session
session = Session()
Base = declarative_base()


class TipoObra(Base):
    __tablename__ = 'tipos_obra'
    tipo_id = Column(Integer, primary_key=True, autoincrement='ignore_fk')
    name = Column(String(50))

    def __repr__(self):
        return "<TipoObra(name='%s')>" % (self.name)


class StatusObra(Base):
    __tablename__ = 'status_obra'
    status_id = Column(Integer, primary_key=True, autoincrement='ignore_fk')
    name = Column(String(50))

    def __repr__(self):
        return "<StatusObra(name='%s')>" % (self.name)


class Orden(Base):
    __tablename__ = 'ordenes'
    orden_id = Column(Integer, primary_key=True, autoincrement='ignore_fk')
    nro_orden = Column(String(50))
    clase_orden = Column(String(100))
    descripcion = Column(String(100))
    ubic_tecnica = Column(String(100))
    ubic_tecnica_desc = Column(String(100))
    geo_x = Column(String(20))
    geo_y = Column(String(20))
    tipo_resultado = Column(String(100))
    fecha_creacion = Column(Date)
    fecha_ini_extremo = Column(Date)
    fecha_fin_extremo = Column(Date)
    calle = Column(String(100))
    altura = Column(String(10))
    clave_modelo = Column(String(50))
    clave_modelo_txt = Column(String(100))
    tipo_obra_id = Column(Integer, ForeignKey('tipos_obra.tipo_id'))
    area_empresa = Column(String(10))
    status_usuario = Column(String(50))
    status_id = Column(Integer, ForeignKey('status_traduccion.status_id'))
    fecha_ult_modif = Column(Date)
    comuna = Column(String(20))

    def __repr__(self):
        return "<Orden(nro_orden='%s', clase_orden='%s', descripcion='%s', ubic_tecnica='%s', ubic_tecnica_desc='%s', geo_x='%s', geo_y='%s', fecha_creacion='%s', fecha_ini_extremo='%s', fecha_fin_extremo='%s', calle='%s', altura='%s', clave_modelo_txt='%s', tipo_obra_id='%s', comuna='%s')>" % (self.nro_orden, self.clase_orden, self.descripcion, self.ubic_tecnica, self.ubic_tecnica_desc, self.geo_x, self.geo_y, self.fecha_creacion,
                                                                                                                                                                                                                                                                                                        self.fecha_ini_extremo, self.fecha_fin_extremo, self.calle, self.altura, self.clave_modelo_txt, self.tipo_obra_id, self.comuna)


class ClaveTipo(Base):
    __tablename__ = 'clave_tipo'
    clave_tipo_id = Column(
        Integer, primary_key=True, autoincrement='ignore_fk')
    clave = Column(String(50))
    tipo_id = Column(Integer, ForeignKey('tipos_obra.tipo_id'))


class StatusTraduccion(Base):
    __tablename__ = 'status_traduccion'
    status_traduccion_id = Column(
        Integer, primary_key=True, autoincrement='ignore_fk')
    status_usuario = Column(String(50))
    status_id = Column(Integer, ForeignKey('status_obra.status_id'))

# Crear todas las tablas necesarias para insertar los datos del WS
Base.metadata.create_all(engine, checkfirst=True)

# Agregar tipo de obra en caso de que este vacia
ev = session.query(TipoObra.name).count()
if not ev:
    session.add_all([TipoObra(name='Bacheo'), TipoObra(name='Senalizacion'), TipoObra(
        name='Tapas'), TipoObra(name='Materiales'), TipoObra(name='Vereda'), TipoObra(
        name='Cordon'), TipoObra(name='Pavimento'), TipoObra(name='Otros')])
    session.commit()


def generar_orden(record):
    columnas = ["NRO_ORDEN", "CLASE_ORDEN", "DESCRIPCION", "UBIC_TECNICA", "UBIC_TECNICA_DESC", "FECHA_CREACION", "FECHA_INI_EXTREMO",
                "FECHA_FIN_EXTREMO",  "CALLE", "ALTURA", "CLAVE_MODELO", "CLAVE_MODELO_TXT", "AREA_EMPRESA", "STATUS_USUARIO", "FECHA_ULT_MODIF"]

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
    return new_orden


def load_data():
    # Agregar status de obra en caso de que este vacia
    ev = session.query(StatusObra.name).count()
    if not ev:
        session.add_all([StatusObra(name='Planificado'), StatusObra(
            name='En ejecucion'), StatusObra(name='Finalizado'), StatusObra(name='Denegado')])
        session.commit()

    # Chequear si ya se agrego el mapeo de clave modelo a tipo de obra
    ev = session.query(ClaveTipo.clave).count()
    if not ev:
        # Si no existen, agregar a la tabla a partir del CSV
        with open('static/data/clave-tipo.csv', 'rb') as csvfile:
            mappings = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in mappings:
                session.add(ClaveTipo(**{"clave": row[0], "tipo_id": row[1]}))
                session.commit()

    # Chequear si ya se agrego el mapeo de status_usuario a status de obra
    # legible por humanos
    ev = session.query(StatusTraduccion.status_usuario).count()
    if not ev:
        # Si no existen, agregar a la tabla a partir del CSV
        with open('static/data/status_usuario-status.csv', 'rb') as csvfile:
            mappings = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in mappings:
                session.add(
                    StatusTraduccion(**{"status_usuario": row[0], "status_id": row[1]}))
                session.commit()
