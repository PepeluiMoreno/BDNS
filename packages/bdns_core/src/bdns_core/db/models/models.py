# File: app/db/models.py
# app/db/models.py


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
    ForeignKey,
    Index,
    Table,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from base  import Base

 
# =========================================================
# MIXINS
# =========================================================

class AuditMixin:
    created_at = Column(DateTime, nullable=False, server_default=func.now(), index=True)
    updated_at = Column(DateTime, onupdate=func.now())
    created_by = Column(String(50))
    updated_by = Column(String(50))


# =========================================================
# ETL
# =========================================================

class EtlJob(Base):
    __tablename__ = "etl_job"

    id = Column(Integer, primary_key=True)

    entity = Column(String(50), nullable=False)      # convocatoria, concesion
    year = Column(Integer, nullable=False)
    mes = Column(Integer)
    tipo = Column(String(1))

    stage = Column(String(20), nullable=False)       # extract, transform, load, sync
    status = Column(String(20), nullable=False, default="pending")

    retries = Column(Integer, nullable=False, default=0)
    last_error = Column(Text)

    started_at = Column(DateTime)
    finished_at = Column(DateTime)
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("entity", "year", "mes", "tipo", "stage", name="uq_etl_job"),
        Index("ix_etl_job_pending", "status", "stage"),
    )


# =========================================================
# CATÁLOGOS
# =========================================================

class Organo(Base):
    __tablename__ = "organo"

    id = Column(String, primary_key=True)
    id_padre = Column(String, ForeignKey("organo.id"))
    nombre = Column(String, nullable=False)

    tipo = Column(String, nullable=False)
    nivel1 = Column(String)
    nivel2 = Column(String)
    nivel3 = Column(String)
    nivel1_norm = Column(String)
    nivel2_norm = Column(String)
    nivel3_norm = Column(String)

    padre = relationship("Organo", remote_side=[id], back_populates="hijos")
    hijos = relationship("Organo", back_populates="padre")

    __table_args__ = (
        Index("ix_organo_niveles", "nivel1_norm", "nivel2_norm", "nivel3_norm"),
    )


class Reglamento(Base):
    __tablename__ = "reglamento"

    id = Column(Integer, primary_key=True)
    descripcion = Column(String)
    descripcion_norm = Column(String)
    ambito = Column(String, nullable=False)


class Finalidad(Base):
    __tablename__ = "finalidad"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String)
    descripcion_norm = Column(String)


class Instrumento(Base):
    __tablename__ = "instrumento"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String)
    descripcion_norm = Column(String)


class Fondo(Base):
    __tablename__ = "fondo"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String)
    descripcion_norm = Column(String)


class Objetivo(Base):
    __tablename__ = "objetivo"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String)
    descripcion_norm = Column(String)


class Region(Base):
    __tablename__ = "region"

    id = Column(Integer, primary_key=True)
    descripcion = Column(String, nullable=False)
    descripcion_norm = Column(String, nullable=False)
    id_padre = Column(Integer, ForeignKey("region.id"))

    padre = relationship("Region", remote_side=[id], back_populates="hijos")
    hijos = relationship("Region", back_populates="padre")


class SectorActividad(Base):
    __tablename__ = "sector_actividad"

    id = Column(String, primary_key=True)
    descripcion = Column(String, nullable=False)
    descripcion_norm = Column(String, index=True)
    id_padre = Column(String, ForeignKey("sector_actividad.id"))

    padre = relationship("SectorActividad", remote_side=[id], back_populates="hijos")
    hijos = relationship("SectorActividad", back_populates="padre")


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


# =========================================================
# BENEFICIARIOS
# =========================================================

class Beneficiario(Base, AuditMixin):
    __tablename__ = "beneficiario"

    id = Column(Integer, primary_key=True)
    nif = Column(String, index=True)
    nombre = Column(String, nullable=False)
    nombre_norm = Column(String, nullable=False)

    tipo_beneficiario_id = Column(Integer, ForeignKey("tipo_beneficiario.id"))
    tipo_beneficiario = relationship("TipoBeneficiario")

    forma_juridica = Column(String)

    pseudonimos = relationship(
        "Pseudonimo",
        back_populates="beneficiario",
        cascade="all, delete-orphan",
    )


class Pseudonimo(Base):
    __tablename__ = "pseudonimo"

    id = Column(Integer, primary_key=True)
    beneficiario_id = Column(Integer, ForeignKey("beneficiario.id"), nullable=False)
    pseudonimo = Column(String, nullable=False)
    pseudonimo_norm = Column(String, nullable=False)

    beneficiario = relationship("Beneficiario", back_populates="pseudonimos")

    __table_args__ = (
        UniqueConstraint("beneficiario_id", "pseudonimo_norm", name="uq_beneficiario_pseudonimo"),
    )


# =========================================================
# CONVOCATORIAS Y CONCESIONES
# =========================================================

class Convocatoria(Base, AuditMixin):
    __tablename__ = "convocatoria"

    id = Column(String, primary_key=True)  # codigo BDNS como string

    descripcion = Column(Text)
    presupuesto_total = Column(Float)
    fecha_recepcion = Column(Date)

    organo_id = Column(String, ForeignKey("organo.id"))
    organo = relationship("Organo")

    reglamento_id = Column(Integer, ForeignKey("reglamento.id"))
    reglamento = relationship("Reglamento")


class Concesion(Base, AuditMixin):
    __tablename__ = "concesion"

    id = Column(String, primary_key=True)
    fecha_concesion = Column(Date)
    importe = Column(Float)

    convocatoria_id = Column(String, ForeignKey("convocatoria.id"), nullable=False)
    convocatoria = relationship("Convocatoria")

    beneficiario_id = Column(Integer, ForeignKey("beneficiario.id"), nullable=False)
    beneficiario = relationship("Beneficiario")


# =========================================================
# SYNC
# =========================================================

class SyncControl(Base, AuditMixin):
    __tablename__ = "sync_control"

    id = Column(Integer, primary_key=True)
    fecha_desde = Column(Date, nullable=False)
    fecha_hasta = Column(Date, nullable=False)

    inserts_detectados = Column(Integer, default=0)
    updates_detectados = Column(Integer, default=0)
    deletes_detectados = Column(Integer, default=0)

    estado = Column(String(20), default="running", index=True)
    error = Column(Text)
