
import strawberry
from typing import List, Optional

# ==================== TYPES ====================

from .types.concesion import Concesion, ConcesionInput
from .types.beneficiario import Beneficiario, BeneficiarioInput
from .types.estadisticas import EstadisticasConcesiones, FiltroEstadisticas
from .types.notificaciones import (
    Usuario,
    Suscripcion,
    Ejecucion,
    TelegramLink,
    FilterDefinition,
    FieldDefinition,
    QueryPreview,
    TestResult,
    EntidadesDisponibles,
)

# ==================== RESOLVERS ====================

from .resolvers.concesiones import (
    get_concesiones,
    get_concesion_by_id,
    get_concesiones_por_beneficiario,
)
from .resolvers.beneficiarios import (
    get_beneficiarios,
    get_beneficiario_by_id,
)
from .resolvers.estadisticas import (
    get_estadisticas_por_tipo_entidad,
    get_estadisticas_por_organo,
    get_concentracion_subvenciones,
)
from .resolvers import notificaciones as notif


# ==================== QUERY ====================

@strawberry.type
class Query:
    @strawberry.field
    async def concesion(self, id: strawberry.ID) -> Optional[Concesion]:
        return await get_concesion_by_id(id)

    @strawberry.field
    async def concesiones(
        self,
        filtros: Optional[ConcesionInput] = None,
        limite: int = 100,
        offset: int = 0,
    ) -> List[Concesion]:
        return await get_concesiones(filtros, limite, offset)

    @strawberry.field
    async def beneficiario(self, id: strawberry.ID) -> Optional[Beneficiario]:
        return await get_beneficiario_by_id(id)

    @strawberry.field
    async def beneficiarios(
        self,
        filtros: Optional[BeneficiarioInput] = None,
        limite: int = 100,
        offset: int = 0,
    ) -> List[Beneficiario]:
        return await get_beneficiarios(filtros, limite, offset)

    @strawberry.field
    async def concesiones_por_beneficiario(
        self,
        beneficiario_id: strawberry.ID,
        anio: Optional[int] = None,
        limite: int = 100,
        offset: int = 0,
    ) -> List[Concesion]:
        return await get_concesiones_por_beneficiario(
            beneficiario_id, anio, limite, offset
        )

    @strawberry.field
    async def estadisticas_por_tipo_entidad(
        self,
        filtros: Optional[FiltroEstadisticas] = None,
    ) -> List[EstadisticasConcesiones]:
        return await get_estadisticas_por_tipo_entidad(filtros)

    @strawberry.field
    async def estadisticas_por_organo(
        self,
        filtros: Optional[FiltroEstadisticas] = None,
    ) -> List[EstadisticasConcesiones]:
        return await get_estadisticas_por_organo(filtros)

    @strawberry.field
    async def concentracion_subvenciones(
        self,
        anio: Optional[int] = None,
        tipo_entidad: Optional[str] = None,
        limite: int = 10,
    ) -> List[EstadisticasConcesiones]:
        return await get_concentracion_subvenciones(
            anio, tipo_entidad, limite
        )

    # ==================== NOTIFICACIONES ====================

    @strawberry.field
    async def usuarios(
        self,
        activo: Optional[bool] = None,
        limite: int = 100,
        offset: int = 0,
    ) -> List[Usuario]:
        return await notif.get_usuarios(activo, limite, offset)

    @strawberry.field
    async def usuario(self, id: int) -> Optional[Usuario]:
        return await notif.get_usuario(id)

    @strawberry.field
    async def suscripciones(
        self,
        usuario_id: Optional[int] = None,
        activo: Optional[bool] = None,
        limite: int = 100,
        offset: int = 0,
    ) -> List[Suscripcion]:
        return await notif.get_suscripciones(
            usuario_id, activo, limite, offset
        )

    @strawberry.field
    async def suscripcion(self, id: int) -> Optional[Suscripcion]:
        return await notif.get_suscripcion(id)

    @strawberry.field
    async def ejecuciones(
        self,
        subscripcion_id: Optional[int] = None,
        estado: Optional[str] = None,
        limite: int = 100,
        offset: int = 0,
    ) -> List[Ejecucion]:
        return await notif.get_ejecuciones(
            subscripcion_id, estado, limite, offset
        )

    # ==================== QUERY BUILDER ====================

    @strawberry.field
    async def entidades_disponibles(self) -> EntidadesDisponibles:
        return await notif.get_entidades_disponibles()

    @strawberry.field
    async def filtros_disponibles(
        self, entity: str
    ) -> List[FilterDefinition]:
        return await notif.get_filtros_disponibles(entity)

    @strawberry.field
    async def campos_disponibles(
        self, entity: str
    ) -> List[FieldDefinition]:
        return await notif.get_campos_disponibles(entity)

    @strawberry.field
    async def preview_query(
        self,
        entity: str,
        filters: strawberry.scalars.JSON,
        fields: Optional[List[str]] = None,
        limite: int = 1000,
    ) -> QueryPreview:
        return await notif.preview_query(
            entity, filters or {}, fields, limite
        )

    @strawberry.field
    async def test_query(
        self,
        entity: str,
        filters: strawberry.scalars.JSON,
        fields: Optional[List[str]] = None,
        limite: int = 100,
    ) -> TestResult:
        return await notif.test_query(
            entity, filters or {}, fields, limite
        )


# ==================== MUTATION ====================

@strawberry.type
class Mutation:
    @strawberry.mutation
    async def crear_usuario(
        self,
        email: str,
        nombre: Optional[str] = None,
    ) -> Usuario:
        return await notif.crear_usuario(email, nombre)

    @strawberry.mutation
    async def actualizar_usuario(
        self,
        id: int,
        email: Optional[str] = None,
        nombre: Optional[str] = None,
        activo: Optional[bool] = None,
    ) -> Optional[Usuario]:
        return await notif.actualizar_usuario(
            id, email, nombre, activo
        )

    @strawberry.mutation
    async def eliminar_usuario(self, id: int) -> bool:
        return await notif.eliminar_usuario(id)

    @strawberry.mutation
    async def generar_link_telegram(
        self, usuario_id: int
    ) -> Optional[TelegramLink]:
        return await notif.generar_link_telegram(usuario_id)

    @strawberry.mutation
    async def crear_suscripcion(
        self,
        usuario_id: int,
        nombre: str,
        graphql_query: str,
        descripcion: Optional[str] = None,
        campo_id: str = "id",
        campos_comparar: Optional[List[str]] = None,
        frecuencia: str = "semanal",
        hora_preferida: int = 8,
    ) -> Optional[Suscripcion]:
        return await notif.crear_suscripcion(
            usuario_id,
            nombre,
            graphql_query,
            descripcion,
            campo_id,
            campos_comparar,
            frecuencia,
            hora_preferida,
        )

    @strawberry.mutation
    async def crear_suscripcion_desde_builder(
        self,
        usuario_id: int,
        nombre: str,
        entity: str,
        filters: strawberry.scalars.JSON,
        descripcion: Optional[str] = None,
        fields: Optional[List[str]] = None,
        limite: int = 1000,
        frecuencia: str = "semanal",
        hora_preferida: int = 8,
    ) -> Optional[Suscripcion]:
        return await notif.crear_suscripcion_desde_builder(
            usuario_id,
            nombre,
            entity,
            filters or {},
            descripcion,
            fields,
            limite,
            frecuencia,
            hora_preferida,
        )

    @strawberry.mutation
    async def actualizar_suscripcion(
        self,
        id: int,
        nombre: Optional[str] = None,
        descripcion: Optional[str] = None,
        graphql_query: Optional[str] = None,
        campo_id: Optional[str] = None,
        campos_comparar: Optional[List[str]] = None,
        frecuencia: Optional[str] = None,
        hora_preferida: Optional[int] = None,
        activo: Optional[bool] = None,
        max_errores: Optional[int] = None,
    ) -> Optional[Suscripcion]:
        return await notif.actualizar_suscripcion(
            id,
            nombre,
            descripcion,
            graphql_query,
            campo_id,
            campos_comparar,
            frecuencia,
            hora_preferida,
            activo,
            max_errores,
        )

    @strawberry.mutation
    async def eliminar_suscripcion(self, id: int) -> bool:
        return await notif.eliminar_suscripcion(id)

    @strawberry.mutation
    async def ejecutar_suscripcion(
        self, id: int
    ) -> Optional[Ejecucion]:
        return await notif.ejecutar_suscripcion(id)

    @strawberry.mutation
    async def reactivar_suscripcion(
        self, id: int
    ) -> Optional[Suscripcion]:
        return await notif.reactivar_suscripcion(id)


# ==================== SCHEMA ====================

schema = strawberry.Schema(query=Query, mutation=Mutation)
