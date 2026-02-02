# File: app/db/models.py

from datetime import datetime
from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    Date,
    DateTime,
    Boolean,
    Text,
    Enum as SQLEnum,
    ForeignKey,
    Index,
    Table,
    UniqueConstraint
)
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from app.db.enums import TipoOrganoEnum, FormaJuridicaEnum, AmbitoReglamentoEnum

Base = declarative_base()


# ================= AUDITORIA Y ETL =================

# NOTA: Los campos de auditoria se definen directamente al final de cada clase
# para garantizar que aparezcan como las ultimas columnas en las tablas.
# Campos estandar:
#   - created_at: DateTime, default=datetime.utcnow, nullable=False, index=True
#   - updated_at: DateTime, onupdate=datetime.utcnow, nullable=True
#   - created_by: String(50), nullable=True
#   - updated_by: String(50), nullable=True


class ETLUser(Base):
    """Usuario/proceso ETL del sistema para auditoria."""
    __tablename__ = "etl_user"

    id = Column(String(36), primary_key=True)
    nombre = Column(String(100), unique=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

# ================= CATÁLOGOS =================

class Anuncio(Base):
    __tablename__ = "anuncio"
    num_anuncio = Column(Integer, primary_key=True)
    titulo = Column(Text)
    titulo_leng = Column(Text)
    texto = Column(Text)
    texto_leng = Column(Text)
    url = Column(String)
    des_diario_oficial = Column(String)
    fecha_publicacion = Column(Date)
    convocatorias = relationship("Convocatoria", secondary="convocatoria_anuncio", back_populates="anuncios")

class Documento(Base):
    __tablename__ = "documento"
    id = Column(Integer, primary_key=True)
    nombre_fic = Column(String)
    descripcion = Column(String)
    longitud = Column(Integer)
    fecha_modificacion = Column(Date)
    fecha_publicacion = Column(Date)
    convocatorias = relationship("Convocatoria", secondary="convocatoria_documento", back_populates="documentos")

class Finalidad(Base):
    __tablename__ = "finalidad"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String)
    descripcion_norm = Column(String)

class Fondo(Base):
    __tablename__ = "fondo"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String)
    descripcion_norm = Column(String)

class Instrumento(Base):
    __tablename__ = "instrumento"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String)
    descripcion_norm = Column(String)

class Objetivo(Base):
    __tablename__ = "objetivo"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String)
    descripcion_norm = Column(String)

class Reglamento(Base):
    __tablename__ = "reglamento"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String)
    descripcion_norm = Column(String)
    ambito = Column(SQLEnum(AmbitoReglamentoEnum, name="ambito_reglamento_enum", native_enum=False), nullable=False)
    
class Region(Base):
    __tablename__ = "region"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String, nullable=False)
    descripcion_norm = Column(String, nullable=False)
    id_padre = Column(Integer, ForeignKey("region.id"), nullable=True)
    padre = relationship("Region", remote_side=[id], backref="hijos")

class SectorActividad(Base):
    __tablename__ = "sector_actividad"
    id = Column(String, primary_key=True)  # ← antes era Integer
    descripcion = Column(String, nullable=False)
    descripcion_norm = Column(String, index=True)
    id_padre = Column(String, ForeignKey("sector_actividad.id"), nullable=True)
    padre = relationship("SectorActividad", remote_side=[id], backref="hijos")

class SectorProducto(Base):
    __tablename__ = "sector_producto"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String)
    descripcion_norm = Column(String)

class TipoBeneficiario(Base):
    __tablename__ = "tipo_beneficiario"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String)
    descripcion_norm = Column(String)

class Organo(Base):
    __tablename__ = "organo"
    id = Column(String, primary_key=True)
    id_padre = Column(String, ForeignKey("organo.id"), nullable=True)
    nombre = Column(String, nullable=False)
    tipo = Column(SQLEnum(TipoOrganoEnum, name="tipo_organo_enum", native_enum=False), nullable=False)
    nivel1 = Column(String, nullable=True)
    nivel2 = Column(String, nullable=True)
    nivel3 = Column(String, nullable=True)
    nivel1_norm = Column(String, nullable=True)
    nivel2_norm = Column(String, nullable=True)
    nivel3_norm = Column(String, nullable=True)
    padre = relationship("Organo", remote_side=[id], backref="hijos")
    __table_args__ = (
        Index("ix_organo_nivel1_nivel2_nivel3", "nivel1_norm", "nivel2_norm", "nivel3_norm"),
    )

class Beneficiario(Base):
    __tablename__ = "beneficiario"
    id = Column(Integer, primary_key=True, autoincrement=False)
    nif = Column(String, index=True)
    nombre = Column(String, nullable=False)
    nombre_norm = Column(String, nullable=False)

    tipo_beneficiario_id = Column(Integer, ForeignKey("tipo_beneficiario.id"))
    tipo_beneficiario = relationship("TipoBeneficiario")

    forma_juridica = Column(SQLEnum(FormaJuridicaEnum, name="forma_juridica_enum", native_enum=False), nullable=True)
    pseudonimos = relationship("Pseudonimo", back_populates="beneficiario", cascade="all, delete-orphan")

    # Campos de auditoria (siempre al final)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    updated_at = Column(DateTime, onupdate=datetime.utcnow, nullable=True)
    created_by = Column(String(50), nullable=True)
    updated_by = Column(String(50), nullable=True)
    
class Pseudonimo(Base):
    __tablename__ = "pseudonimo"
    id = Column(Integer, primary_key=True, autoincrement=True)
    beneficiario_id = Column(Integer, ForeignKey("beneficiario.id"), nullable=False, index=True)
    pseudonimo = Column(String, nullable=False)
    pseudonimo_norm = Column(String, nullable=False)
   
    __table_args__ = (UniqueConstraint('beneficiario_id', 'pseudonimo_norm', name='uq_beneficiario_pseudonimo'),)

    beneficiario = relationship("Beneficiario", back_populates="pseudonimos")
    
# ================= RELACIONES N:M =================

convocatoria_anuncio = Table(
    "convocatoria_anuncio", Base.metadata,
    Column("convocatoria_id", Integer, ForeignKey("convocatoria.id"), primary_key=True),
    Column("anuncio_id", Integer, ForeignKey("anuncio.num_anuncio"), primary_key=True),
)

convocatoria_documento = Table(
    "convocatoria_documento", Base.metadata,
    Column("convocatoria_id", Integer, ForeignKey("convocatoria.id"), primary_key=True),
    Column("documento_id", Integer, ForeignKey("documento.id"), primary_key=True),
)

convocatoria_finalidad = Table(
    "convocatoria_finalidad", Base.metadata,
    Column("convocatoria_id", Integer, ForeignKey("convocatoria.id"), primary_key=True),
    Column("finalidad_id", Integer, ForeignKey("finalidad.id"), primary_key=True),
)

convocatoria_fondo = Table(
    "convocatoria_fondo", Base.metadata,
    Column("convocatoria_id", Integer, ForeignKey("convocatoria.id"), primary_key=True),
    Column("fondo_id", Integer, ForeignKey("fondo.id"), primary_key=True),
)

convocatoria_instrumento = Table(
    "convocatoria_instrumento", Base.metadata,
    Column("convocatoria_id", Integer, ForeignKey("convocatoria.id"), primary_key=True),
    Column("instrumento_id", Integer, ForeignKey("instrumento.id"), primary_key=True),
)

convocatoria_objetivo = Table(
    "convocatoria_objetivo", Base.metadata,
    Column("convocatoria_id", Integer, ForeignKey("convocatoria.id"), primary_key=True),
    Column("objetivo_id", Integer, ForeignKey("objetivo.id"), primary_key=True),
)

convocatoria_region = Table(
    "convocatoria_region", Base.metadata,
    Column("convocatoria_id", Integer, ForeignKey("convocatoria.id"), primary_key=True),
    Column("region_id", Integer, ForeignKey("region.id"), primary_key=True),
)

convocatoria_sector_actividad = Table(
    "convocatoria_sector_actividad", Base.metadata,
    Column("convocatoria_id", Integer, ForeignKey("convocatoria.id"), primary_key=True),
    Column("sector_actividad_id", String, ForeignKey("sector_actividad.id"), primary_key=True),
)

convocatoria_sector_producto = Table(
    "convocatoria_sector_producto", Base.metadata,
    Column("convocatoria_id", Integer, ForeignKey("convocatoria.id"), primary_key=True),
    Column("sector_producto_id", Integer, ForeignKey("sector_producto.id"), primary_key=True),
)

convocatoria_tipo_beneficiario = Table(
    "convocatoria_tipo_beneficiario", Base.metadata,
    Column("convocatoria_id", Integer, ForeignKey("convocatoria.id"), primary_key=True),
    Column("tipo_beneficiario_id", Integer, ForeignKey("tipo_beneficiario.id"), primary_key=True),
)

# ================= CONVOCATORIA =================

class Convocatoria(Base):
    __tablename__ = "convocatoria"
    id = Column(Integer, primary_key=True, autoincrement=False)  #Usamos el codigoBDNS
    descripcion = Column(Text)
    descripcion_leng = Column(Text)
    descripcion_finalidad = Column(Text)
    descripcion_bases = Column(Text)
    url_bases = Column(String)
    url_ayuda_estado = Column(String)
    ayuda_estado = Column(String)
    tipo_convocatoria = Column(String)
    sede_electronica = Column(String)
    abierto = Column(Boolean)
    se_publica_diario_oficial = Column(Boolean)
    presupuesto_total = Column(Float)
    mrr = Column(Boolean)
    fecha_recepcion = Column(Date)
    fecha_inicio_solicitud = Column(Date)
    fecha_fin_solicitud = Column(Date)

    organo_id = Column(String, ForeignKey("organo.id"))
    organo = relationship("Organo", backref="convocatorias")

    reglamento_id = Column(Integer, ForeignKey("reglamento.id"))
    reglamento = relationship("Reglamento")

    finalidad_id = Column(Integer, ForeignKey("finalidad.id"))
    finalidad = relationship("Finalidad")

    instrumentos = relationship("Instrumento", secondary="convocatoria_instrumento")
    tipos_beneficiarios = relationship("TipoBeneficiario", secondary="convocatoria_tipo_beneficiario")
    sectores_actividad = relationship("SectorActividad", secondary="convocatoria_sector_actividad")
    sectores_producto = relationship("SectorProducto", secondary="convocatoria_sector_producto")
    regiones = relationship("Region", secondary="convocatoria_region")
    finalidades = relationship("Finalidad", secondary="convocatoria_finalidad")
    objetivos = relationship("Objetivo", secondary="convocatoria_objetivo")
    documentos = relationship("Documento", secondary="convocatoria_documento", back_populates="convocatorias")
    anuncios = relationship("Anuncio", secondary="convocatoria_anuncio", back_populates="convocatorias")
    fondos = relationship("Fondo", secondary="convocatoria_fondo")

    # Campos de auditoria (siempre al final)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    updated_at = Column(DateTime, onupdate=datetime.utcnow, nullable=True)
    created_by = Column(String(50), nullable=True)
    updated_by = Column(String(50), nullable=True)

class Concesion(Base):
    __tablename__ = "concesion"
    id = Column(String, primary_key=True, autoincrement=False)
    fecha_concesion = Column(Date)
    importe = Column(Float)
    ayuda_equivalente = Column(Float)
    url_br = Column(String)
    tiene_proyecto = Column(Boolean)

    # Relación con convocatoria por codigo_bdns (string, NO por id)
    codigo_bdns = Column(Integer, ForeignKey("convocatoria.id"), nullable=False, index=True)
    convocatoria = relationship("Convocatoria", backref="concesiones", foreign_keys=[codigo_bdns])

    id_beneficiario = Column(Integer, ForeignKey("beneficiario.id"), nullable=False, index=True)
    beneficiario = relationship("Beneficiario", backref="concesiones")
    id_instrumento = Column(Integer, ForeignKey("instrumento.id"))
    instrumento = relationship("Instrumento")

    # Campos de auditoria (siempre al final)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    updated_at = Column(DateTime, onupdate=datetime.utcnow, nullable=True)
    created_by = Column(String(50), nullable=True)
    updated_by = Column(String(50), nullable=True)

class Minimi(Base):
    __tablename__ = "minimi"
    id = Column(String, primary_key=True, autoincrement=False)  # Igual que id_concesion
    fecha_concesion = Column(Date, index=True)
    fecha_registro = Column(Date, index=True)
    ayuda_equivalente = Column(Float)

    # Relación con concesión (por id, string)
    concesion_id = Column(String, ForeignKey("concesion.id"), nullable=False, index=True)
    concesion = relationship("Concesion", backref="minimis")

    # Relación con convocatoria por codigo_bdns (string)
    codigo_bdns = Column(Integer, ForeignKey("convocatoria.id"), nullable=False, index=True)
    convocatoria = relationship("Convocatoria", backref="minimis", foreign_keys=[codigo_bdns])

    id_beneficiario = Column(Integer, ForeignKey("beneficiario.id"), nullable=False, index=True)
    beneficiario = relationship("Beneficiario", backref="minimis")
    id_instrumento = Column(Integer, ForeignKey("instrumento.id"), nullable=True)
    instrumento = relationship("Instrumento")
    id_reglamento = Column(Integer, ForeignKey("reglamento.id"), nullable=True)
    reglamento = relationship("Reglamento")
    id_sector_actividad = Column(String, ForeignKey("sector_actividad.id"), nullable=True)
    sector_actividad = relationship("SectorActividad")
    id_sector_producto = Column(Integer, ForeignKey("sector_producto.id"), nullable=True)
    sector_producto = relationship("SectorProducto")

    # Campos de auditoria (siempre al final)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    updated_at = Column(DateTime, onupdate=datetime.utcnow, nullable=True)
    created_by = Column(String(50), nullable=True)
    updated_by = Column(String(50), nullable=True)

class AyudaEstado(Base):
    __tablename__ = "ayuda_estado"
    id = Column(Integer, primary_key=True, autoincrement=False)
    fecha_concesion = Column(Date, index=True)
    fecha_registro = Column(Date, index=True)
    ayuda_equivalente = Column(Float)
    ayuda_estado = Column(Integer, index=True)
    url_ayuda_estado = Column(String)
    entidad = Column(String)
    intermediario = Column(String)

    # Relación con concesión (por id, string)
    concesion_id = Column(String, ForeignKey("concesion.id"), nullable=False)
    concesion = relationship("Concesion", backref="ayudas_estado")

    # Relación con convocatoria por codigo_bdns (string)
    codigo_bdns = Column(Integer, ForeignKey("convocatoria.id"), nullable=False, index=True)
    convocatoria = relationship("Convocatoria", backref="ayudas_estado", foreign_keys=[codigo_bdns])

    id_beneficiario = Column(Integer, ForeignKey("beneficiario.id"), nullable=False, index=True)
    beneficiario = relationship("Beneficiario", backref="ayudas_estado")

    id_instrumento = Column(Integer, ForeignKey("instrumento.id"), nullable=True)
    instrumento = relationship("Instrumento")

    id_reglamento = Column(Integer, ForeignKey("reglamento.id"), nullable=True)
    reglamento = relationship("Reglamento")

    id_sector_actividad = Column(String, ForeignKey("sector_actividad.id"), nullable=True)
    sector_actividad = relationship("SectorActividad")

    id_sector_producto = Column(Integer, ForeignKey("sector_producto.id"), nullable=True)
    sector_producto = relationship("SectorProducto")

    region_id = Column(Integer, ForeignKey("region.id"), nullable=True)
    region = relationship("Region")

    objetivo_id = Column(Integer, ForeignKey("objetivo.id"), nullable=True)
    objetivo = relationship("Objetivo")

    tipo_beneficiario_id = Column(Integer, ForeignKey("tipo_beneficiario.id"), nullable=True)
    tipo_beneficiario = relationship("TipoBeneficiario")

    # Campos de auditoria (siempre al final)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    updated_at = Column(DateTime, onupdate=datetime.utcnow, nullable=True)
    created_by = Column(String(50), nullable=True)
    updated_by = Column(String(50), nullable=True)


# ================= ESTADISTICAS Y SYNC =================

class BeneficiarioEstadisticasAnuales(Base):
    """
    Estadisticas agregadas por beneficiario/ejercicio/organo.

    Esta tabla se actualiza automaticamente mediante triggers PostgreSQL
    cuando se insertan, actualizan o eliminan concesiones.
    """
    __tablename__ = "beneficiario_estadisticas_anuales"

    id = Column(Integer, primary_key=True, autoincrement=True)
    beneficiario_id = Column(Integer, ForeignKey("beneficiario.id"), nullable=False, index=True)
    ejercicio = Column(Integer, nullable=False, index=True)
    organo_id = Column(String, ForeignKey("organo.id"), nullable=False, index=True)

    # Metricas
    num_concesiones = Column(Integer, nullable=False, default=0)
    importe_total = Column(Float, nullable=False, default=0.0)
    importe_medio = Column(Float, nullable=False, default=0.0)
    fecha_primera_concesion = Column(Date, nullable=True)
    fecha_ultima_concesion = Column(Date, nullable=True)

    # Relaciones
    beneficiario = relationship("Beneficiario", backref="estadisticas_anuales")
    organo = relationship("Organo")

    __table_args__ = (
        UniqueConstraint('beneficiario_id', 'ejercicio', 'organo_id',
                        name='uq_beneficiario_ejercicio_organo'),
        Index('ix_estadisticas_ejercicio_organo', 'ejercicio', 'organo_id'),
    )

    # Campos de auditoria (siempre al final)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, onupdate=datetime.utcnow, nullable=True)


class SyncControl(Base):
    """
    Control de sincronizacion con BDNS.

    Registra cada ejecucion del proceso de sincronizacion mensual
    que detecta cambios en BDNS (nuevas concesiones, modificadas, eliminadas).
    """
    __tablename__ = "sync_control"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fecha_ejecucion = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    ventana_meses = Column(Integer, nullable=False, default=48)
    fecha_desde = Column(Date, nullable=False)
    fecha_hasta = Column(Date, nullable=False)

    # Resultados de deteccion
    total_api = Column(Integer, nullable=True)
    total_local = Column(Integer, nullable=True)
    inserts_detectados = Column(Integer, default=0)
    updates_detectados = Column(Integer, default=0)
    deletes_detectados = Column(Integer, default=0)

    # Resultados de aplicacion
    inserts_aplicados = Column(Integer, default=0)
    updates_aplicados = Column(Integer, default=0)
    deletes_aplicados = Column(Integer, default=0)

    # Estado
    estado = Column(String(20), default='running', index=True)  # running, completed, failed
    error = Column(Text, nullable=True)

    # Campos de auditoria (siempre al final)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, onupdate=datetime.utcnow, nullable=True)


# ================= USUARIOS Y NOTIFICACIONES =================

class Usuario(Base):
    """
    Usuario del sistema con vinculacion a Telegram.

    Permite a usuarios registrarse y vincular su cuenta de Telegram
    para recibir notificaciones personalizadas.
    """
    __tablename__ = "usuario"

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    nombre = Column(String(200), nullable=True)

    # Vinculacion Telegram
    telegram_chat_id = Column(String(50), unique=True, nullable=True, index=True)
    telegram_username = Column(String(100), nullable=True)
    telegram_verificado = Column(Boolean, default=False)
    telegram_token_verificacion = Column(String(64), nullable=True)  # Token temporal para vincular

    # Estado
    activo = Column(Boolean, default=True)

    # Relacion con suscripciones
    suscripciones = relationship("SubscripcionNotificacion", back_populates="usuario", cascade="all, delete-orphan")

    # Campos de auditoria (siempre al final)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    updated_at = Column(DateTime, onupdate=datetime.utcnow, nullable=True)


class SubscripcionNotificacion(Base):
    """
    Suscripcion a notificaciones basada en queries GraphQL.

    El usuario define una query GraphQL que actua como filtro.
    El monitor ejecuta periodicamente la query y compara resultados
    con el snapshot anterior para detectar cambios (CRUD).
    """
    __tablename__ = "subscripcion_notificacion"

    id = Column(Integer, primary_key=True, autoincrement=True)
    usuario_id = Column(Integer, ForeignKey("usuario.id"), nullable=False, index=True)

    # Identificacion
    nombre = Column(String(200), nullable=False)  # Ej: "Subvenciones a religiosos 2024"
    descripcion = Column(Text, nullable=True)

    # Query GraphQL que define el filtro
    graphql_query = Column(Text, nullable=False)

    # Campo identificador para detectar cambios (ej: "id", "codigo_bdns")
    campo_id = Column(String(50), default="id", nullable=False)

    # Campos a comparar para detectar modificaciones
    campos_comparar = Column(JSONB, nullable=True)  # ["importe", "fecha_concesion"]

    # Snapshot del ultimo resultado
    last_result_hash = Column(String(64), nullable=True)  # SHA256 del resultado
    last_results = Column(JSONB, nullable=True)  # Resultados completos (dict por ID)
    last_check = Column(DateTime, nullable=True)
    last_check_count = Column(Integer, nullable=True)  # Numero de registros

    # Scheduling
    frecuencia = Column(String(20), default='semanal')  # diaria, semanal, mensual
    hora_preferida = Column(Integer, default=8)  # Hora UTC preferida (0-23)
    proxima_ejecucion = Column(DateTime, nullable=True, index=True)

    # Control de errores
    errores_consecutivos = Column(Integer, default=0)
    max_errores = Column(Integer, default=3)  # Desactivar tras N errores
    ultimo_error = Column(Text, nullable=True)

    # Estado
    activo = Column(Boolean, default=True, index=True)
    pausado_por_errores = Column(Boolean, default=False)

    # Relaciones
    usuario = relationship("Usuario", back_populates="suscripciones")
    ejecuciones = relationship("EjecucionNotificacion", back_populates="subscripcion", cascade="all, delete-orphan")

    # Campos de auditoria (siempre al final)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    updated_at = Column(DateTime, onupdate=datetime.utcnow, nullable=True)


class EjecucionNotificacion(Base):
    """
    Registro de cada ejecucion del monitor para una suscripcion.

    Guarda historico de ejecuciones, cambios detectados y notificaciones enviadas.
    """
    __tablename__ = "ejecucion_notificacion"

    id = Column(Integer, primary_key=True, autoincrement=True)
    subscripcion_id = Column(Integer, ForeignKey("subscripcion_notificacion.id"), nullable=False, index=True)

    # Resultado de la ejecucion
    fecha_ejecucion = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    estado = Column(String(20), default='ejecutando')  # ejecutando, completado, error

    # Metricas
    registros_actuales = Column(Integer, nullable=True)
    registros_anteriores = Column(Integer, nullable=True)

    # Cambios detectados
    nuevos = Column(Integer, default=0)
    modificados = Column(Integer, default=0)
    eliminados = Column(Integer, default=0)

    # Detalle de cambios (opcional, para auditoria)
    detalle_cambios = Column(JSONB, nullable=True)  # {nuevos: [...], modificados: [...], eliminados: [...]}

    # Notificacion
    notificacion_enviada = Column(Boolean, default=False)
    mensaje_enviado = Column(Text, nullable=True)

    # Error si hubo
    error = Column(Text, nullable=True)

    # Relaciones
    subscripcion = relationship("SubscripcionNotificacion", back_populates="ejecuciones")

    # Campos de auditoria (siempre al final)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)