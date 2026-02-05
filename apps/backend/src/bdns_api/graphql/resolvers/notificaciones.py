"""
Resolvers GraphQL para el sistema de notificaciones.

Queries y Mutations para gestionar usuarios, suscripciones y el query builder.
"""
import sys
from pathlib import Path
from typing import List, Optional
from datetime import datetime

# Setup path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from bdns_core.db.session import get_db_context
from bdns_core.db.models import Usuario, SubscripcionNotificacion, EjecucionNotificacion
from app.graphql.types.notificaciones import (
    Usuario as UsuarioType,
    Suscripcion as SuscripcionType,
    Ejecucion as EjecucionType,
    TelegramLink,
    FilterDefinition,
    FilterOption,
    FieldDefinition,
    QueryPreview,
    TestResult,
    EntidadesDisponibles,
)


def _usuario_to_type(u) -> UsuarioType:
    """Convierte modelo a tipo GraphQL."""
    return UsuarioType(
        id=str(u.id),
        email=u.email,
        nombre=u.nombre,
        telegram_chat_id=u.telegram_chat_id,
        telegram_username=u.telegram_username,
        telegram_verificado=u.telegram_verificado,
        activo=u.activo,
        created_at=u.created_at,
    )


def _suscripcion_to_type(s) -> SuscripcionType:
    """Convierte modelo a tipo GraphQL."""
    return SuscripcionType(
        id=str(s.id),
        usuario_id=s.usuario_id,
        nombre=s.nombre,
        descripcion=s.descripcion,
        graphql_query=s.graphql_query,
        campo_id=s.campo_id,
        campos_comparar=s.campos_comparar,
        frecuencia=s.frecuencia,
        hora_preferida=s.hora_preferida,
        activo=s.activo,
        pausado_por_errores=s.pausado_por_errores,
        errores_consecutivos=s.errores_consecutivos,
        ultimo_error=s.ultimo_error,
        last_check=s.last_check,
        last_check_count=s.last_check_count,
        proxima_ejecucion=s.proxima_ejecucion,
        created_at=s.created_at,
    )


def _ejecucion_to_type(e) -> EjecucionType:
    """Convierte modelo a tipo GraphQL."""
    return EjecucionType(
        id=str(e.id),
        subscripcion_id=e.subscripcion_id,
        fecha_ejecucion=e.fecha_ejecucion,
        estado=e.estado,
        registros_actuales=e.registros_actuales,
        registros_anteriores=e.registros_anteriores,
        nuevos=e.nuevos,
        modificados=e.modificados,
        eliminados=e.eliminados,
        notificacion_enviada=e.notificacion_enviada,
        error=e.error,
        created_at=e.created_at,
    )


# ==================== QUERIES: USUARIOS ====================

async def get_usuarios(
    activo: Optional[bool] = None,
    limite: int = 100,
    offset: int = 0
) -> List[UsuarioType]:
    """Lista usuarios."""
    with get_db_context() as db:
        query = db.query(Usuario)
        if activo is not None:
            query = query.filter(Usuario.activo == activo)
        usuarios = query.offset(offset).limit(limite).all()
        return [_usuario_to_type(u) for u in usuarios]


async def get_usuario(id: int) -> Optional[UsuarioType]:
    """Obtiene un usuario por ID."""
    with get_db_context() as db:
        u = db.query(Usuario).filter(Usuario.id == id).first()
        return _usuario_to_type(u) if u else None


# ==================== QUERIES: SUSCRIPCIONES ====================

async def get_suscripciones(
    usuario_id: Optional[int] = None,
    activo: Optional[bool] = None,
    limite: int = 100,
    offset: int = 0
) -> List[SuscripcionType]:
    """Lista suscripciones."""
    with get_db_context() as db:
        query = db.query(SubscripcionNotificacion)
        if usuario_id is not None:
            query = query.filter(SubscripcionNotificacion.usuario_id == usuario_id)
        if activo is not None:
            query = query.filter(SubscripcionNotificacion.activo == activo)
        subs = query.order_by(SubscripcionNotificacion.created_at.desc()).offset(offset).limit(limite).all()
        return [_suscripcion_to_type(s) for s in subs]


async def get_suscripcion(id: int) -> Optional[SuscripcionType]:
    """Obtiene una suscripcion por ID."""
    with get_db_context() as db:
        s = db.query(SubscripcionNotificacion).filter(SubscripcionNotificacion.id == id).first()
        return _suscripcion_to_type(s) if s else None


# ==================== QUERIES: EJECUCIONES ====================

async def get_ejecuciones(
    subscripcion_id: Optional[int] = None,
    estado: Optional[str] = None,
    limite: int = 100,
    offset: int = 0
) -> List[EjecucionType]:
    """Lista historial de ejecuciones."""
    with get_db_context() as db:
        query = db.query(EjecucionNotificacion)
        if subscripcion_id is not None:
            query = query.filter(EjecucionNotificacion.subscripcion_id == subscripcion_id)
        if estado is not None:
            query = query.filter(EjecucionNotificacion.estado == estado)
        ejecs = query.order_by(EjecucionNotificacion.fecha_ejecucion.desc()).offset(offset).limit(limite).all()
        return [_ejecucion_to_type(e) for e in ejecs]


# ==================== QUERIES: QUERY BUILDER ====================

async def get_entidades_disponibles() -> EntidadesDisponibles:
    """Lista entidades disponibles para suscripcion."""
    from telegram_notifications.query_builder import QueryBuilder
    return EntidadesDisponibles(
        entities=list(QueryBuilder.ENTITIES.keys()),
        default="concesiones"
    )


async def get_filtros_disponibles(entity: str) -> List[FilterDefinition]:
    """Obtiene filtros disponibles para una entidad."""
    from telegram_notifications.query_builder import QueryBuilder

    if entity not in QueryBuilder.ENTITIES:
        return []

    filtros = QueryBuilder.get_available_filters(entity)
    result = []
    for f in filtros:
        options = None
        if f.get("options"):
            options = [FilterOption(value=o["value"], label=o["label"]) for o in f["options"]]
        result.append(FilterDefinition(
            name=f["name"],
            label=f["label"],
            type=f["type"],
            options=options,
            description=f["description"],
            ejemplo=f["ejemplo"],
            graphql_param=f["graphql_param"],
        ))
    return result


async def get_campos_disponibles(entity: str) -> List[FieldDefinition]:
    """Obtiene campos seleccionables para una entidad."""
    from telegram_notifications.query_builder import QueryBuilder

    if entity not in QueryBuilder.ENTITIES:
        return []

    campos = QueryBuilder.get_available_fields(entity)
    return [FieldDefinition(name=k, description=v) for k, v in campos.items()]


async def preview_query(
    entity: str,
    filters: dict,
    fields: Optional[List[str]] = None,
    limite: int = 1000
) -> QueryPreview:
    """Genera preview de query GraphQL."""
    from telegram_notifications.query_builder import QueryBuilder

    builder = QueryBuilder(entity=entity, limite=limite)

    for key, value in (filters or {}).items():
        if value is not None and value != "":
            builder.add_filter(key, value)

    if fields:
        builder.select_fields(fields)

    preview = builder.preview()
    return QueryPreview(
        query=preview["query"],
        entity=preview["entity"],
        filters_active=preview["filters_active"],
        fields_selected=preview["fields_selected"],
        fields=preview["fields"],
    )


async def test_query(
    entity: str,
    filters: dict,
    fields: Optional[List[str]] = None,
    limite: int = 100
) -> TestResult:
    """Ejecuta query en modo test."""
    import time
    from telegram_notifications.query_builder import QueryBuilder
    from telegram_notifications.monitor import execute_graphql_query, extract_records

    builder = QueryBuilder(entity=entity, limite=min(limite, 100))

    for key, value in (filters or {}).items():
        if value is not None and value != "":
            builder.add_filter(key, value)

    if fields:
        builder.select_fields(fields)

    query = builder.build()

    start = time.time()
    data, error = execute_graphql_query(query)
    duration_ms = (time.time() - start) * 1000

    if error:
        return TestResult(
            registros=0,
            primeros_registros=[],
            query_ejecutada=query,
            duracion_ms=round(duration_ms, 2),
        )

    records = extract_records(data, "id")

    return TestResult(
        registros=len(records),
        primeros_registros=list(records.values())[:10],
        query_ejecutada=query,
        duracion_ms=round(duration_ms, 2),
    )


# ==================== MUTATIONS: USUARIOS ====================

async def crear_usuario(email: str, nombre: Optional[str] = None) -> UsuarioType:
    """Crea un nuevo usuario."""
    with get_db_context() as db:
        # Verificar email unico
        if db.query(Usuario).filter(Usuario.email == email).first():
            raise Exception("Email ya registrado")

        u = Usuario(email=email, nombre=nombre)
        db.add(u)
        db.commit()
        db.refresh(u)
        return _usuario_to_type(u)


async def actualizar_usuario(
    id: int,
    email: Optional[str] = None,
    nombre: Optional[str] = None,
    activo: Optional[bool] = None
) -> Optional[UsuarioType]:
    """Actualiza un usuario."""
    with get_db_context() as db:
        u = db.query(Usuario).filter(Usuario.id == id).first()
        if not u:
            return None

        if email is not None:
            u.email = email
        if nombre is not None:
            u.nombre = nombre
        if activo is not None:
            u.activo = activo

        db.commit()
        db.refresh(u)
        return _usuario_to_type(u)


async def eliminar_usuario(id: int) -> bool:
    """Elimina un usuario y sus suscripciones."""
    with get_db_context() as db:
        u = db.query(Usuario).filter(Usuario.id == id).first()
        if not u:
            return False
        db.delete(u)
        db.commit()
        return True


async def generar_link_telegram(usuario_id: int) -> Optional[TelegramLink]:
    """Genera token para vincular Telegram."""
    import secrets

    with get_db_context() as db:
        u = db.query(Usuario).filter(Usuario.id == usuario_id).first()
        if not u:
            return None

        token = secrets.token_urlsafe(32)
        u.telegram_token_verificacion = token
        u.telegram_verificado = False
        db.commit()

        return TelegramLink(
            token=token,
            instrucciones=(
                f"Envia este mensaje al bot de Telegram @BDNS_NotificadorBot:\n\n"
                f"/vincular {token}\n\n"
                f"El token expira en 24 horas."
            )
        )


# ==================== MUTATIONS: SUSCRIPCIONES ====================

async def crear_suscripcion(
    usuario_id: int,
    nombre: str,
    graphql_query: str,
    descripcion: Optional[str] = None,
    campo_id: str = "id",
    campos_comparar: Optional[List[str]] = None,
    frecuencia: str = "semanal",
    hora_preferida: int = 8
) -> Optional[SuscripcionType]:
    """Crea una nueva suscripcion."""
    with get_db_context() as db:
        # Verificar usuario
        if not db.query(Usuario).filter(Usuario.id == usuario_id).first():
            return None

        s = SubscripcionNotificacion(
            usuario_id=usuario_id,
            nombre=nombre,
            descripcion=descripcion,
            graphql_query=graphql_query,
            campo_id=campo_id,
            campos_comparar=campos_comparar,
            frecuencia=frecuencia,
            hora_preferida=hora_preferida,
        )
        db.add(s)
        db.commit()
        db.refresh(s)
        return _suscripcion_to_type(s)


async def crear_suscripcion_desde_builder(
    usuario_id: int,
    nombre: str,
    entity: str,
    filters: dict,
    descripcion: Optional[str] = None,
    fields: Optional[List[str]] = None,
    limite: int = 1000,
    frecuencia: str = "semanal",
    hora_preferida: int = 8
) -> Optional[SuscripcionType]:
    """Crea suscripcion desde el query builder."""
    from telegram_notifications.query_builder import QueryBuilder

    with get_db_context() as db:
        # Verificar usuario
        if not db.query(Usuario).filter(Usuario.id == usuario_id).first():
            return None

        # Construir query
        builder = QueryBuilder(entity=entity, limite=limite)
        for key, value in (filters or {}).items():
            if value is not None and value != "":
                builder.add_filter(key, value)
        if fields:
            builder.select_fields(fields)

        query = builder.build()

        s = SubscripcionNotificacion(
            usuario_id=usuario_id,
            nombre=nombre,
            descripcion=descripcion,
            graphql_query=query,
            campo_id="id",
            campos_comparar=fields,
            frecuencia=frecuencia,
            hora_preferida=hora_preferida,
        )
        db.add(s)
        db.commit()
        db.refresh(s)
        return _suscripcion_to_type(s)


async def actualizar_suscripcion(
    id: int,
    nombre: Optional[str] = None,
    descripcion: Optional[str] = None,
    graphql_query: Optional[str] = None,
    campo_id: Optional[str] = None,
    campos_comparar: Optional[List[str]] = None,
    frecuencia: Optional[str] = None,
    hora_preferida: Optional[int] = None,
    activo: Optional[bool] = None,
    max_errores: Optional[int] = None
) -> Optional[SuscripcionType]:
    """Actualiza una suscripcion."""
    with get_db_context() as db:
        s = db.query(SubscripcionNotificacion).filter(SubscripcionNotificacion.id == id).first()
        if not s:
            return None

        if nombre is not None:
            s.nombre = nombre
        if descripcion is not None:
            s.descripcion = descripcion
        if graphql_query is not None:
            s.graphql_query = graphql_query
        if campo_id is not None:
            s.campo_id = campo_id
        if campos_comparar is not None:
            s.campos_comparar = campos_comparar
        if frecuencia is not None:
            s.frecuencia = frecuencia
        if hora_preferida is not None:
            s.hora_preferida = hora_preferida
        if max_errores is not None:
            s.max_errores = max_errores
        if activo is not None:
            s.activo = activo
            if activo:
                s.pausado_por_errores = False
                s.errores_consecutivos = 0

        db.commit()
        db.refresh(s)
        return _suscripcion_to_type(s)


async def eliminar_suscripcion(id: int) -> bool:
    """Elimina una suscripcion."""
    with get_db_context() as db:
        s = db.query(SubscripcionNotificacion).filter(SubscripcionNotificacion.id == id).first()
        if not s:
            return False
        db.delete(s)
        db.commit()
        return True


async def ejecutar_suscripcion(id: int) -> Optional[EjecucionType]:
    """Ejecuta manualmente una suscripcion."""
    from telegram_notifications.monitor import process_subscription

    with get_db_context() as db:
        s = db.query(SubscripcionNotificacion).filter(SubscripcionNotificacion.id == id).first()
        if not s:
            return None

        process_subscription(db, s)

        # Obtener ultima ejecucion
        e = db.query(EjecucionNotificacion).filter(
            EjecucionNotificacion.subscripcion_id == id
        ).order_by(EjecucionNotificacion.id.desc()).first()

        return _ejecucion_to_type(e) if e else None


async def reactivar_suscripcion(id: int) -> Optional[SuscripcionType]:
    """Reactiva una suscripcion pausada por errores."""
    with get_db_context() as db:
        s = db.query(SubscripcionNotificacion).filter(SubscripcionNotificacion.id == id).first()
        if not s:
            return None

        s.activo = True
        s.pausado_por_errores = False
        s.errores_consecutivos = 0
        s.ultimo_error = None

        db.commit()
        db.refresh(s)
        return _suscripcion_to_type(s)
