"""add etl_user and audit fields

Revision ID: a1b2c3d4e5f6
Revises: 6ce38c918174
Create Date: 2026-01-31

Agrega:
- Tabla etl_user para tracking del proceso ETL
- Campos de auditoria (created_at, updated_at, created_by, updated_by) a:
  - beneficiario
  - convocatoria
  - concesion
  - minimi
  - ayuda_estado
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a1b2c3d4e5f6'
down_revision = '6ce38c918174'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Crear tabla etl_user
    op.create_table(
        'etl_user',
        sa.Column('id', sa.String(36), primary_key=True),
        sa.Column('nombre', sa.String(100), unique=True, nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=True),
    )

    # Agregar campos de auditoria a beneficiario
    op.add_column('beneficiario', sa.Column('created_at', sa.DateTime(), nullable=True, index=True))
    op.add_column('beneficiario', sa.Column('updated_at', sa.DateTime(), nullable=True))
    op.add_column('beneficiario', sa.Column('created_by', sa.String(50), nullable=True))
    op.add_column('beneficiario', sa.Column('updated_by', sa.String(50), nullable=True))

    # Agregar campos de auditoria a convocatoria
    op.add_column('convocatoria', sa.Column('created_at', sa.DateTime(), nullable=True, index=True))
    op.add_column('convocatoria', sa.Column('updated_at', sa.DateTime(), nullable=True))
    op.add_column('convocatoria', sa.Column('created_by', sa.String(50), nullable=True))
    op.add_column('convocatoria', sa.Column('updated_by', sa.String(50), nullable=True))

    # Agregar campos de auditoria a concesion
    op.add_column('concesion', sa.Column('created_at', sa.DateTime(), nullable=True, index=True))
    op.add_column('concesion', sa.Column('updated_at', sa.DateTime(), nullable=True))
    op.add_column('concesion', sa.Column('created_by', sa.String(50), nullable=True))
    op.add_column('concesion', sa.Column('updated_by', sa.String(50), nullable=True))

    # Agregar campos de auditoria a minimi
    op.add_column('minimi', sa.Column('created_at', sa.DateTime(), nullable=True, index=True))
    op.add_column('minimi', sa.Column('updated_at', sa.DateTime(), nullable=True))
    op.add_column('minimi', sa.Column('created_by', sa.String(50), nullable=True))
    op.add_column('minimi', sa.Column('updated_by', sa.String(50), nullable=True))

    # Agregar campos de auditoria a ayuda_estado
    op.add_column('ayuda_estado', sa.Column('created_at', sa.DateTime(), nullable=True, index=True))
    op.add_column('ayuda_estado', sa.Column('updated_at', sa.DateTime(), nullable=True))
    op.add_column('ayuda_estado', sa.Column('created_by', sa.String(50), nullable=True))
    op.add_column('ayuda_estado', sa.Column('updated_by', sa.String(50), nullable=True))

    # NOTA: Los indices se crean automaticamente con index=True en add_column


def downgrade() -> None:
    # NOTA: Los indices se eliminan automaticamente al eliminar las columnas

    # Eliminar campos de ayuda_estado
    op.drop_column('ayuda_estado', 'updated_by')
    op.drop_column('ayuda_estado', 'created_by')
    op.drop_column('ayuda_estado', 'updated_at')
    op.drop_column('ayuda_estado', 'created_at')

    # Eliminar campos de minimi
    op.drop_column('minimi', 'updated_by')
    op.drop_column('minimi', 'created_by')
    op.drop_column('minimi', 'updated_at')
    op.drop_column('minimi', 'created_at')

    # Eliminar campos de concesion
    op.drop_column('concesion', 'updated_by')
    op.drop_column('concesion', 'created_by')
    op.drop_column('concesion', 'updated_at')
    op.drop_column('concesion', 'created_at')

    # Eliminar campos de convocatoria
    op.drop_column('convocatoria', 'updated_by')
    op.drop_column('convocatoria', 'created_by')
    op.drop_column('convocatoria', 'updated_at')
    op.drop_column('convocatoria', 'created_at')

    # Eliminar campos de beneficiario
    op.drop_column('beneficiario', 'updated_by')
    op.drop_column('beneficiario', 'created_by')
    op.drop_column('beneficiario', 'updated_at')
    op.drop_column('beneficiario', 'created_at')

    # Eliminar tabla etl_user
    op.drop_table('etl_user')
