"""add sync_control table

Revision ID: d4e5f6g7h8i9
Revises: c3d4e5f6g7h8
Create Date: 2026-02-01

Agrega tabla sync_control para control de sincronizacion
mensual con BDNS.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd4e5f6g7h8i9'
down_revision = 'c3d4e5f6g7h8'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'sync_control',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('fecha_ejecucion', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('ventana_meses', sa.Integer(), nullable=False, server_default='48'),
        sa.Column('fecha_desde', sa.Date(), nullable=False),
        sa.Column('fecha_hasta', sa.Date(), nullable=False),
        # Resultados de deteccion
        sa.Column('total_api', sa.Integer(), nullable=True),
        sa.Column('total_local', sa.Integer(), nullable=True),
        sa.Column('inserts_detectados', sa.Integer(), server_default='0'),
        sa.Column('updates_detectados', sa.Integer(), server_default='0'),
        sa.Column('deletes_detectados', sa.Integer(), server_default='0'),
        # Resultados de aplicacion
        sa.Column('inserts_aplicados', sa.Integer(), server_default='0'),
        sa.Column('updates_aplicados', sa.Integer(), server_default='0'),
        sa.Column('deletes_aplicados', sa.Integer(), server_default='0'),
        # Estado
        sa.Column('estado', sa.String(20), server_default='running'),
        sa.Column('error', sa.Text(), nullable=True),
        # Auditoria (siempre al final)
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
    )

    # Indices
    op.create_index('ix_sync_control_fecha_ejecucion', 'sync_control', ['fecha_ejecucion'])
    op.create_index('ix_sync_control_estado', 'sync_control', ['estado'])


def downgrade() -> None:
    op.drop_index('ix_sync_control_estado', 'sync_control')
    op.drop_index('ix_sync_control_fecha_ejecucion', 'sync_control')
    op.drop_table('sync_control')
