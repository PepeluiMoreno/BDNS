"""add beneficiario_estadisticas_anuales table

Revision ID: b2c3d4e5f6g7
Revises: a1b2c3d4e5f6
Create Date: 2026-02-01

Agrega tabla beneficiario_estadisticas_anuales para estadisticas
agregadas por beneficiario/ejercicio/organo.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b2c3d4e5f6g7'
down_revision = 'a1b2c3d4e5f6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'beneficiario_estadisticas_anuales',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('beneficiario_id', sa.Integer(), sa.ForeignKey('beneficiario.id'), nullable=False),
        sa.Column('ejercicio', sa.Integer(), nullable=False),
        sa.Column('organo_id', sa.String(), sa.ForeignKey('organo.id'), nullable=False),
        # Metricas
        sa.Column('num_concesiones', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('importe_total', sa.Float(), nullable=False, server_default='0.0'),
        sa.Column('importe_medio', sa.Float(), nullable=False, server_default='0.0'),
        sa.Column('fecha_primera_concesion', sa.Date(), nullable=True),
        sa.Column('fecha_ultima_concesion', sa.Date(), nullable=True),
        # Auditoria (siempre al final)
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
    )

    # Constraint unico: beneficiario + ejercicio + organo
    op.create_unique_constraint(
        'uq_beneficiario_ejercicio_organo',
        'beneficiario_estadisticas_anuales',
        ['beneficiario_id', 'ejercicio', 'organo_id']
    )

    # Indices
    op.create_index('ix_estadisticas_beneficiario_id', 'beneficiario_estadisticas_anuales', ['beneficiario_id'])
    op.create_index('ix_estadisticas_ejercicio', 'beneficiario_estadisticas_anuales', ['ejercicio'])
    op.create_index('ix_estadisticas_organo_id', 'beneficiario_estadisticas_anuales', ['organo_id'])
    op.create_index('ix_estadisticas_ejercicio_organo', 'beneficiario_estadisticas_anuales', ['ejercicio', 'organo_id'])


def downgrade() -> None:
    op.drop_index('ix_estadisticas_ejercicio_organo', 'beneficiario_estadisticas_anuales')
    op.drop_index('ix_estadisticas_organo_id', 'beneficiario_estadisticas_anuales')
    op.drop_index('ix_estadisticas_ejercicio', 'beneficiario_estadisticas_anuales')
    op.drop_index('ix_estadisticas_beneficiario_id', 'beneficiario_estadisticas_anuales')
    op.drop_constraint('uq_beneficiario_ejercicio_organo', 'beneficiario_estadisticas_anuales')
    op.drop_table('beneficiario_estadisticas_anuales')
