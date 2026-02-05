#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
API REST para gestion de suscripciones a notificaciones.

Endpoints:
    GET    /api/notificaciones/usuarios              - Listar usuarios
    POST   /api/notificaciones/usuarios              - Crear usuario
    GET    /api/notificaciones/usuarios/{id}         - Obtener usuario
    PUT    /api/notificaciones/usuarios/{id}         - Actualizar usuario
    DELETE /api/notificaciones/usuarios/{id}         - Eliminar usuario

    GET    /api/notificaciones/suscripciones         - Listar suscripciones
    POST   /api/notificaciones/suscripciones         - Crear suscripcion
    GET    /api/notificaciones/suscripciones/{id}    - Obtener suscripcion
    PUT    /api/notificaciones/suscripciones/{id}    - Actualizar suscripcion
    DELETE /api/notificaciones/suscripciones/{id}    - Eliminar suscripcion
    POST   /api/notificaciones/suscripciones/{id}/test - Probar suscripcion

    GET    /api/notificaciones/ejecuciones           - Historial ejecuciones
"""
import os
import sys
import secrets
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

# Setup path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from bdns_core.db.session import get_db
from bdns_core.db.models import Usuario, SubscripcionNotificacion, EjecucionNotificacion

router = APIRouter(prefix="/api/notificaciones", tags=["notificaciones"])


# ==================== SCHEMAS ====================

class UsuarioBase(BaseModel):
    email: str
    nombre: Optional[str] = None


class UsuarioCreate(UsuarioBase):
    pass


class UsuarioUpdate(BaseModel):
    email: Optional[str] = None
    nombre: Optional[str] = None
    activo: Optional[bool] = None


class UsuarioResponse(UsuarioBase):
    id: int
    telegram_chat_id: Optional[str] = None
    telegram_username: Optional[str] = None
    telegram_verificado: bool = False
    activo: bool = True
    created_at: datetime

    class Config:
        from_attributes = True


class SubscripcionBase(BaseModel):
    nombre: str
    descripcion: Optional[str] = None
    graphql_query: str
    campo_id: str = "id"
    campos_comparar: Optional[List[str]] = None
    frecuencia: str = "semanal"  # diaria, semanal, mensual
    hora_preferida: int = Field(default=8, ge=0, le=23)


class SubscripcionCreate(SubscripcionBase):
    usuario_id: int


class SubscripcionUpdate(BaseModel):
    nombre: Optional[str] = None
    descripcion: Optional[str] = None
    graphql_query: Optional[str] = None
    campo_id: Optional[str] = None
    campos_comparar: Optional[List[str]] = None
    frecuencia: Optional[str] = None
    hora_preferida: Optional[int] = None
    activo: Optional[bool] = None
    max_errores: Optional[int] = None


class SubscripcionResponse(SubscripcionBase):
    id: int
    usuario_id: int
    activo: bool = True
    pausado_por_errores: bool = False
    errores_consecutivos: int = 0
    ultimo_error: Optional[str] = None
    last_check: Optional[datetime] = None
    last_check_count: Optional[int] = None
    proxima_ejecucion: Optional[datetime] = None
    created_at: datetime

    class Config:
        from_attributes = True


class EjecucionResponse(BaseModel):
    id: int
    subscripcion_id: int
    fecha_ejecucion: datetime
    estado: str
    registros_actuales: Optional[int] = None
    registros_anteriores: Optional[int] = None
    nuevos: int = 0
    modificados: int = 0
    eliminados: int = 0
    notificacion_enviada: bool = False
    error: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


class TestResult(BaseModel):
    registros: int
    primeros_10: List[dict]
    query_ejecutada: str
    duracion_ms: float


class TelegramLinkRequest(BaseModel):
    """Solicitud para vincular Telegram."""
    pass


class TelegramLinkResponse(BaseModel):
    """Token de verificacion para vincular Telegram."""
    token: str
    instrucciones: str


class QueryBuilderRequest(BaseModel):
    """Solicitud para construir query GraphQL."""
    entity: str = "concesiones"  # concesiones, beneficiarios
    filters: dict = {}
    fields: Optional[List[str]] = None
    limite: int = 1000


class QueryPreviewResponse(BaseModel):
    """Preview de query GraphQL generada."""
    query: str
    entity: str
    filters_active: int
    filters: dict
    fields_selected: int
    fields: List[str]
    pagination: dict


class FilterDefinitionResponse(BaseModel):
    """Definicion de un filtro disponible."""
    name: str
    label: str
    type: str
    options: Optional[List[dict]] = None
    description: str
    ejemplo: str
    graphql_param: str


# ==================== USUARIOS ====================

@router.get("/usuarios", response_model=List[UsuarioResponse])
def listar_usuarios(
    skip: int = 0,
    limit: int = 100,
    activo: Optional[bool] = None,
    db: Session = Depends(get_db)
):
    """Lista todos los usuarios."""
    query = db.query(Usuario)
    if activo is not None:
        query = query.filter(Usuario.activo == activo)
    return query.offset(skip).limit(limit).all()


@router.post("/usuarios", response_model=UsuarioResponse, status_code=201)
def crear_usuario(usuario: UsuarioCreate, db: Session = Depends(get_db)):
    """Crea un nuevo usuario."""
    # Verificar email unico
    if db.query(Usuario).filter(Usuario.email == usuario.email).first():
        raise HTTPException(400, "Email ya registrado")

    db_usuario = Usuario(**usuario.model_dump())
    db.add(db_usuario)
    db.commit()
    db.refresh(db_usuario)
    return db_usuario


@router.get("/usuarios/{usuario_id}", response_model=UsuarioResponse)
def obtener_usuario(usuario_id: int, db: Session = Depends(get_db)):
    """Obtiene un usuario por ID."""
    usuario = db.query(Usuario).filter(Usuario.id == usuario_id).first()
    if not usuario:
        raise HTTPException(404, "Usuario no encontrado")
    return usuario


@router.put("/usuarios/{usuario_id}", response_model=UsuarioResponse)
def actualizar_usuario(
    usuario_id: int,
    datos: UsuarioUpdate,
    db: Session = Depends(get_db)
):
    """Actualiza un usuario."""
    usuario = db.query(Usuario).filter(Usuario.id == usuario_id).first()
    if not usuario:
        raise HTTPException(404, "Usuario no encontrado")

    for key, value in datos.model_dump(exclude_unset=True).items():
        setattr(usuario, key, value)

    db.commit()
    db.refresh(usuario)
    return usuario


@router.delete("/usuarios/{usuario_id}", status_code=204)
def eliminar_usuario(usuario_id: int, db: Session = Depends(get_db)):
    """Elimina un usuario y todas sus suscripciones."""
    usuario = db.query(Usuario).filter(Usuario.id == usuario_id).first()
    if not usuario:
        raise HTTPException(404, "Usuario no encontrado")

    db.delete(usuario)
    db.commit()


@router.post("/usuarios/{usuario_id}/telegram/link", response_model=TelegramLinkResponse)
def generar_link_telegram(usuario_id: int, db: Session = Depends(get_db)):
    """
    Genera un token para vincular cuenta de Telegram.

    El usuario debe enviar este token al bot de Telegram para verificar.
    """
    usuario = db.query(Usuario).filter(Usuario.id == usuario_id).first()
    if not usuario:
        raise HTTPException(404, "Usuario no encontrado")

    # Generar token aleatorio
    token = secrets.token_urlsafe(32)
    usuario.telegram_token_verificacion = token
    usuario.telegram_verificado = False
    db.commit()

    return TelegramLinkResponse(
        token=token,
        instrucciones=(
            f"Envia este mensaje al bot de Telegram @BDNS_NotificadorBot:\n\n"
            f"/vincular {token}\n\n"
            f"El token expira en 24 horas."
        )
    )


# ==================== SUSCRIPCIONES ====================

@router.get("/suscripciones", response_model=List[SubscripcionResponse])
def listar_suscripciones(
    skip: int = 0,
    limit: int = 100,
    usuario_id: Optional[int] = None,
    activo: Optional[bool] = None,
    db: Session = Depends(get_db)
):
    """Lista suscripciones con filtros opcionales."""
    query = db.query(SubscripcionNotificacion)

    if usuario_id is not None:
        query = query.filter(SubscripcionNotificacion.usuario_id == usuario_id)
    if activo is not None:
        query = query.filter(SubscripcionNotificacion.activo == activo)

    return query.order_by(SubscripcionNotificacion.created_at.desc()).offset(skip).limit(limit).all()


@router.post("/suscripciones", response_model=SubscripcionResponse, status_code=201)
def crear_suscripcion(sub: SubscripcionCreate, db: Session = Depends(get_db)):
    """Crea una nueva suscripcion."""
    # Verificar que el usuario existe
    usuario = db.query(Usuario).filter(Usuario.id == sub.usuario_id).first()
    if not usuario:
        raise HTTPException(404, "Usuario no encontrado")

    db_sub = SubscripcionNotificacion(**sub.model_dump())
    db.add(db_sub)
    db.commit()
    db.refresh(db_sub)
    return db_sub


@router.get("/suscripciones/{sub_id}", response_model=SubscripcionResponse)
def obtener_suscripcion(sub_id: int, db: Session = Depends(get_db)):
    """Obtiene una suscripcion por ID."""
    sub = db.query(SubscripcionNotificacion).filter(SubscripcionNotificacion.id == sub_id).first()
    if not sub:
        raise HTTPException(404, "Suscripcion no encontrada")
    return sub


@router.put("/suscripciones/{sub_id}", response_model=SubscripcionResponse)
def actualizar_suscripcion(
    sub_id: int,
    datos: SubscripcionUpdate,
    db: Session = Depends(get_db)
):
    """Actualiza una suscripcion."""
    sub = db.query(SubscripcionNotificacion).filter(SubscripcionNotificacion.id == sub_id).first()
    if not sub:
        raise HTTPException(404, "Suscripcion no encontrada")

    for key, value in datos.model_dump(exclude_unset=True).items():
        setattr(sub, key, value)

    # Si se reactiva, resetear errores
    if datos.activo is True:
        sub.pausado_por_errores = False
        sub.errores_consecutivos = 0

    db.commit()
    db.refresh(sub)
    return sub


@router.delete("/suscripciones/{sub_id}", status_code=204)
def eliminar_suscripcion(sub_id: int, db: Session = Depends(get_db)):
    """Elimina una suscripcion y su historial."""
    sub = db.query(SubscripcionNotificacion).filter(SubscripcionNotificacion.id == sub_id).first()
    if not sub:
        raise HTTPException(404, "Suscripcion no encontrada")

    db.delete(sub)
    db.commit()


@router.post("/suscripciones/{sub_id}/test", response_model=TestResult)
def test_suscripcion(
    sub_id: int,
    limit: int = Query(default=100, le=1000),
    db: Session = Depends(get_db)
):
    """
    Ejecuta la query de una suscripcion en modo test.

    Devuelve los primeros N registros sin guardar ni notificar.
    Util para validar que la query es correcta antes de activar.
    """
    import time
    from .monitor import execute_graphql_query, extract_records

    sub = db.query(SubscripcionNotificacion).filter(SubscripcionNotificacion.id == sub_id).first()
    if not sub:
        raise HTTPException(404, "Suscripcion no encontrada")

    start = time.time()
    data, error = execute_graphql_query(sub.graphql_query)
    duration_ms = (time.time() - start) * 1000

    if error:
        raise HTTPException(400, f"Error ejecutando query: {error}")

    records = extract_records(data, sub.campo_id)

    return TestResult(
        registros=len(records),
        primeros_10=list(records.values())[:min(10, limit)],
        query_ejecutada=sub.graphql_query,
        duracion_ms=round(duration_ms, 2)
    )


@router.post("/suscripciones/{sub_id}/ejecutar", response_model=EjecucionResponse)
def ejecutar_suscripcion(sub_id: int, db: Session = Depends(get_db)):
    """
    Ejecuta manualmente una suscripcion.

    Detecta cambios y envia notificacion si aplica.
    """
    from .monitor import process_subscription

    sub = db.query(SubscripcionNotificacion).filter(SubscripcionNotificacion.id == sub_id).first()
    if not sub:
        raise HTTPException(404, "Suscripcion no encontrada")

    process_subscription(db, sub)

    # Obtener ultima ejecucion
    ejecucion = db.query(EjecucionNotificacion).filter(
        EjecucionNotificacion.subscripcion_id == sub_id
    ).order_by(EjecucionNotificacion.id.desc()).first()

    return ejecucion


@router.post("/suscripciones/{sub_id}/reactivar", response_model=SubscripcionResponse)
def reactivar_suscripcion(sub_id: int, db: Session = Depends(get_db)):
    """Reactiva una suscripcion pausada por errores."""
    sub = db.query(SubscripcionNotificacion).filter(SubscripcionNotificacion.id == sub_id).first()
    if not sub:
        raise HTTPException(404, "Suscripcion no encontrada")

    sub.activo = True
    sub.pausado_por_errores = False
    sub.errores_consecutivos = 0
    sub.ultimo_error = None

    db.commit()
    db.refresh(sub)
    return sub


# ==================== EJECUCIONES ====================

@router.get("/ejecuciones", response_model=List[EjecucionResponse])
def listar_ejecuciones(
    skip: int = 0,
    limit: int = 100,
    subscripcion_id: Optional[int] = None,
    estado: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Lista historial de ejecuciones."""
    query = db.query(EjecucionNotificacion)

    if subscripcion_id is not None:
        query = query.filter(EjecucionNotificacion.subscripcion_id == subscripcion_id)
    if estado is not None:
        query = query.filter(EjecucionNotificacion.estado == estado)

    return query.order_by(EjecucionNotificacion.fecha_ejecucion.desc()).offset(skip).limit(limit).all()


@router.get("/ejecuciones/{ejecucion_id}")
def obtener_ejecucion(ejecucion_id: int, db: Session = Depends(get_db)):
    """Obtiene detalle de una ejecucion, incluyendo cambios detectados."""
    ej = db.query(EjecucionNotificacion).filter(EjecucionNotificacion.id == ejecucion_id).first()
    if not ej:
        raise HTTPException(404, "Ejecucion no encontrada")

    return {
        **EjecucionResponse.model_validate(ej).model_dump(),
        "detalle_cambios": ej.detalle_cambios,
        "mensaje_enviado": ej.mensaje_enviado
    }


# ==================== QUERY BUILDER ====================

@router.get("/query-builder/entities")
def listar_entidades():
    """Lista las entidades disponibles para suscripcion."""
    from .query_builder import QueryBuilder
    return {
        "entities": list(QueryBuilder.ENTITIES.keys()),
        "default": "concesiones"
    }


@router.get("/query-builder/filters/{entity}", response_model=List[FilterDefinitionResponse])
def obtener_filtros_disponibles(entity: str):
    """
    Obtiene los filtros disponibles para una entidad.

    Devuelve lista de filtros con sus tipos, opciones y descripciones
    para que el UI pueda renderizar los controles apropiados.
    """
    from .query_builder import QueryBuilder

    if entity not in QueryBuilder.ENTITIES:
        raise HTTPException(400, f"Entidad no soportada: {entity}")

    return QueryBuilder.get_available_filters(entity)


@router.get("/query-builder/fields/{entity}")
def obtener_campos_disponibles(entity: str):
    """
    Obtiene los campos seleccionables para una entidad.

    Devuelve dict de campo -> descripcion.
    """
    from .query_builder import QueryBuilder

    if entity not in QueryBuilder.ENTITIES:
        raise HTTPException(400, f"Entidad no soportada: {entity}")

    fields = QueryBuilder.get_available_fields(entity)
    default_fields = QueryBuilder.ENTITIES[entity]["default_fields"]

    return {
        "fields": fields,
        "default_fields": default_fields
    }


@router.post("/query-builder/preview", response_model=QueryPreviewResponse)
def preview_query(request: QueryBuilderRequest):
    """
    Genera preview de la query GraphQL basada en los filtros seleccionados.

    El UI debe llamar a este endpoint cada vez que el usuario modifica
    un filtro para mostrar la query resultante en tiempo real.
    """
    from .query_builder import QueryBuilder

    if request.entity not in QueryBuilder.ENTITIES:
        raise HTTPException(400, f"Entidad no soportada: {request.entity}")

    builder = QueryBuilder(
        entity=request.entity,
        limite=request.limite
    )

    for key, value in request.filters.items():
        if value is not None and value != "":
            builder.add_filter(key, value)

    if request.fields:
        builder.select_fields(request.fields)

    preview = builder.preview()
    return QueryPreviewResponse(**preview)


@router.post("/query-builder/test", response_model=TestResult)
def test_query_builder(request: QueryBuilderRequest):
    """
    Ejecuta la query construida en modo test.

    Devuelve los primeros registros para validar que el filtro es correcto.
    """
    import time
    from .query_builder import QueryBuilder
    from .monitor import execute_graphql_query, extract_records

    if request.entity not in QueryBuilder.ENTITIES:
        raise HTTPException(400, f"Entidad no soportada: {request.entity}")

    builder = QueryBuilder(
        entity=request.entity,
        limite=min(request.limite, 100)  # Limitar para test
    )

    for key, value in request.filters.items():
        if value is not None and value != "":
            builder.add_filter(key, value)

    if request.fields:
        builder.select_fields(request.fields)

    query = builder.build()

    start = time.time()
    data, error = execute_graphql_query(query)
    duration_ms = (time.time() - start) * 1000

    if error:
        raise HTTPException(400, f"Error ejecutando query: {error}")

    config = QueryBuilder.ENTITIES[request.entity]
    campo_id = config.get("campo_id", "id")
    records = extract_records(data, campo_id)

    return TestResult(
        registros=len(records),
        primeros_10=list(records.values())[:10],
        query_ejecutada=query,
        duracion_ms=round(duration_ms, 2)
    )


@router.post("/query-builder/crear-suscripcion", response_model=SubscripcionResponse, status_code=201)
def crear_suscripcion_desde_builder(
    request: QueryBuilderRequest,
    nombre: str = Query(..., description="Nombre de la suscripcion"),
    usuario_id: int = Query(..., description="ID del usuario"),
    descripcion: Optional[str] = Query(None),
    frecuencia: str = Query("semanal"),
    hora_preferida: int = Query(8, ge=0, le=23),
    db: Session = Depends(get_db)
):
    """
    Crea una suscripcion directamente desde el query builder.

    Combina la generacion de query con la creacion de suscripcion
    en un solo paso para facilitar el flujo del UI.
    """
    from .query_builder import QueryBuilder

    if request.entity not in QueryBuilder.ENTITIES:
        raise HTTPException(400, f"Entidad no soportada: {request.entity}")

    # Verificar usuario
    usuario = db.query(Usuario).filter(Usuario.id == usuario_id).first()
    if not usuario:
        raise HTTPException(404, "Usuario no encontrado")

    # Construir query
    builder = QueryBuilder(
        entity=request.entity,
        limite=request.limite
    )

    for key, value in request.filters.items():
        if value is not None and value != "":
            builder.add_filter(key, value)

    if request.fields:
        builder.select_fields(request.fields)

    query = builder.build()

    # Crear suscripcion
    sub = SubscripcionNotificacion(
        usuario_id=usuario_id,
        nombre=nombre,
        descripcion=descripcion,
        graphql_query=query,
        campo_id="id",
        campos_comparar=request.fields,
        frecuencia=frecuencia,
        hora_preferida=hora_preferida
    )

    db.add(sub)
    db.commit()
    db.refresh(sub)

    return sub
