import sys
from pathlib import Path

# Agregar el path de bdns_core al sys.path
alembic_dir = Path(__file__).parent.resolve()
backend_dir = alembic_dir.parent.resolve()
project_root = backend_dir.parent.parent.resolve()
bdns_core_src = project_root / "packages" / "bdns_core" / "src"

if str(bdns_core_src) not in sys.path:
    sys.path.insert(0, str(bdns_core_src))

# Ahora los imports funcionan
from bdns_core.db.base import Base
from bdns_core.db.models.models import *


target_metadata = Base.metadata

print(Base.metadata.tables.keys())

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

def run_migrations_offline():
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online():
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()