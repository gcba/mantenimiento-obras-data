from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import config

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
