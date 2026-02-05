from sqlalchemy import (
    BigInteger,
    Integer,
    Text,
    CHAR,
    TIMESTAMP,
    UniqueConstraint,
    Index,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column
from bdns_core.db.base import Base


class EtlJob(Base):
    __tablename__ = "etl_job"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    entity: Mapped[str] = mapped_column(Text, nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    mes: Mapped[int | None] = mapped_column(Integer)
    tipo: Mapped[str | None] = mapped_column(CHAR(1))

    stage: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="pending")

    retries: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text)

    started_at: Mapped = mapped_column(TIMESTAMP)
    finished_at: Mapped = mapped_column(TIMESTAMP)
    updated_at: Mapped = mapped_column(
        TIMESTAMP,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint(
            "entity", "year", "mes", "tipo", "stage",
            name="uq_etl_job_scope",
        ),
        Index("idx_etl_job_pending", "status", "stage"),
        Index("idx_etl_job_scope", "entity", "year", "mes", "tipo"),
    )

