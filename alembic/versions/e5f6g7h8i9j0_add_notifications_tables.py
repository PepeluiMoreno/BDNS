"""add notifications tables

Revision ID: e5f6g7h8i9j0
Revises: d4e5f6g7h8i9
Create Date: 2026-02-01

Agrega tablas para sistema de notificaciones:
- usuario: usuarios con vinculacion a Telegram
- subscripcion_notificacion: suscripciones a queries GraphQL
- ejecucion_notificacion: historial de ejecuciones del monitor
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers, used by Alembic.
revision = 'e5f6g7h8i9j0'
down_revision = 'd4e5f6g7h8i9'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Tabla usuario
    op.create_table(
        'usuario',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('email', sa.String(255), unique=True, nullable=False),
        sa.Column('nombre', sa.String(200), nullable=True),
        # Vinculacion Telegram
        sa.Column('telegram_chat_id', sa.String(50), unique=True, nullable=True),
        sa.Column('telegram_username', sa.String(100), nullable=True),
        sa.Column('telegram_verificado', sa.Boolean(), server_default='false'),
        sa.Column('telegram_token_verificacion', sa.String(64), nullable=True),
        # Estado
        sa.Column('activo', sa.Boolean(), server_default='true'),
        # Auditoria
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
    )
    op.create_index('ix_usuario_email', 'usuario', ['email'])
    op.create_index('ix_usuario_telegram_chat_id', 'usuario', ['telegram_chat_id'])
    op.create_index('ix_usuario_created_at', 'usuario', ['created_at'])

    # Tabla subscripcion_notificacion
    op.create_table(
        'subscripcion_notificacion',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('usuario_id', sa.Integer(), sa.ForeignKey('usuario.id'), nullable=False),
        # Identificacion
        sa.Column('nombre', sa.String(200), nullable=False),
        sa.Column('descripcion', sa.Text(), nullable=True),
        # Query GraphQL
        sa.Column('graphql_query', sa.Text(), nullable=False),
        sa.Column('campo_id', sa.String(50), server_default='id', nullable=False),
        sa.Column('campos_comparar', JSONB, nullable=True),
        # Snapshot
        sa.Column('last_result_hash', sa.String(64), nullable=True),
        sa.Column('last_results', JSONB, nullable=True),
        sa.Column('last_check', sa.DateTime(), nullable=True),
        sa.Column('last_check_count', sa.Integer(), nullable=True),
        # Scheduling
        sa.Column('frecuencia', sa.String(20), server_default='semanal'),
        sa.Column('hora_preferida', sa.Integer(), server_default='8'),
        sa.Column('proxima_ejecucion', sa.DateTime(), nullable=True),
        # Control de errores
        sa.Column('errores_consecutivos', sa.Integer(), server_default='0'),
        sa.Column('max_errores', sa.Integer(), server_default='3'),
        sa.Column('ultimo_error', sa.Text(), nullable=True),
        # Estado
        sa.Column('activo', sa.Boolean(), server_default='true'),
        sa.Column('pausado_por_errores', sa.Boolean(), server_default='false'),
        # Auditoria
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
    )
    op.create_index('ix_subscripcion_usuario_id', 'subscripcion_notificacion', ['usuario_id'])
    op.create_index('ix_subscripcion_activo', 'subscripcion_notificacion', ['activo'])
    op.create_index('ix_subscripcion_proxima_ejecucion', 'subscripcion_notificacion', ['proxima_ejecucion'])
    op.create_index('ix_subscripcion_created_at', 'subscripcion_notificacion', ['created_at'])

    # Tabla ejecucion_notificacion
    op.create_table(
        'ejecucion_notificacion',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('subscripcion_id', sa.Integer(), sa.ForeignKey('subscripcion_notificacion.id'), nullable=False),
        # Resultado
        sa.Column('fecha_ejecucion', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('estado', sa.String(20), server_default='ejecutando'),
        # Metricas
        sa.Column('registros_actuales', sa.Integer(), nullable=True),
        sa.Column('registros_anteriores', sa.Integer(), nullable=True),
        # Cambios
        sa.Column('nuevos', sa.Integer(), server_default='0'),
        sa.Column('modificados', sa.Integer(), server_default='0'),
        sa.Column('eliminados', sa.Integer(), server_default='0'),
        sa.Column('detalle_cambios', JSONB, nullable=True),
        # Notificacion
        sa.Column('notificacion_enviada', sa.Boolean(), server_default='false'),
        sa.Column('mensaje_enviado', sa.Text(), nullable=True),
        # Error
        sa.Column('error', sa.Text(), nullable=True),
        # Auditoria
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index('ix_ejecucion_subscripcion_id', 'ejecucion_notificacion', ['subscripcion_id'])
    op.create_index('ix_ejecucion_fecha', 'ejecucion_notificacion', ['fecha_ejecucion'])


def downgrade() -> None:
    op.drop_table('ejecucion_notificacion')
    op.drop_table('subscripcion_notificacion')
    op.drop_table('usuario')
