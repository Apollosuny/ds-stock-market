from sqlalchemy.orm import declarative_base
from sqlalchemy import (
    PrimaryKeyConstraint,
    Column,
    DateTime,
    String,
    func,
)

base = declarative_base()


class Products(base):
    __tablename__ = "products"
    __table_args__ = (PrimaryKeyConstraint("sku", name="sku"),)

    sku = Column(String, unique=True)
    style = Column(String)
    category = Column(String)
    size = Column(String)
    asin = Column(String, unique=True)
    createdAt = Column(DateTime, default=func.now())
    updatedAt = Column(DateTime, default=func.now(), onupdate=func.now())
