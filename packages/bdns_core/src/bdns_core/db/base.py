# bdns_core/db/base.py

from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import MetaData
from sqlalchemy.sql import func
from sqlalchemy import DateTime, Column, String

metadata = MetaData(schema="bdns")  # o configurable por env

class Base(DeclarativeBase):
    metadata = metadata



