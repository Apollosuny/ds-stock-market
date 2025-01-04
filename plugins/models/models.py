# import enum
from sqlalchemy import PrimaryKeyConstraint

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    func,
)
from sqlalchemy.orm import declarative_base, relationship

base = declarative_base()


class Product(base):
    __tablename__ = "product"

    productId = Column(Integer, primary_key=True, autoincrement=True)
    sku = Column(String, unique=True)
    style = Column(String)
    category = Column(String)
    size = Column(String)
    asin = Column(String)
    createdAt = Column(DateTime, default=func.now())
    updatedAt = Column(DateTime, default=func.now(), onupdate=func.now())


class Customer(base):
    __tablename__ = "customer"

    customerId = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    email = Column(String)
    phone = Column(String)
    country = Column(String)
    city = Column(String)
    state = Column(String)
    postalCode = Column(String)
    createdAt = Column(DateTime, default=func.now())
    updatedAt = Column(DateTime, default=func.now(), onupdate=func.now())

    # relationships
    orders = relationship("Order", back_populates="customer")



class Order(base):
    __tablename__ = "order"

    id = Column(Integer, primary_key=True)
    orderId = Column(String, unique=True)
    date = Column(DateTime)
    B2B = Column(Boolean)
    status = Column(String)
    fullfillment = Column(String)
    orderChannel = Column(String)
    promotionIds = Column(String)
    createdAt = Column(DateTime, default=func.now())
    updatedAt = Column(DateTime, default=func.now(), onupdate=func.now())

    # relationships
    customerId = Column(
        Integer, ForeignKey("customer.customerId"), nullable=False  # Changed to Integer for consistency
    )
    customer = relationship("Customer", back_populates="order")
    order_detail = relationship("OrderDetail", back_populates="order")
    shipping = relationship("Shipping", back_populates="order")

class OrderDetail(base):
    __tablename__ = "order_detail"

    id = Column(Integer, primary_key=True)
    orderId = Column(String, ForeignKey("order.orderId"), nullable=False)
    productId = Column(String, ForeignKey("product.sku"), nullable=False)
    quantity = Column(Integer)
    amount = Column(Float)
    currency = Column(String)
    createdAt = Column(DateTime, default=func.now())
    updatedAt = Column(DateTime, default=func.now(), onupdate=func.now())

    # relationships
    order = relationship("Order", back_populates="order_detail")
    product = relationship("Product", back_populates="order_detail")


class Shipping(base):
    __tablename__ = "shipping"
    __table_args__ = (PrimaryKeyConstraint("shippingId", name="shippingId"),)

    shippingId = Column(Integer, primary_key=True, autoincrement=True)
    orderId = Column(String, ForeignKey("order.orderId"), nullable=False)
    shipServiceLevel = Column(String)
    courierStatus = Column(String)
    city = Column(String)
    state = Column(String)
    postalCode = Column(String)
    country = Column(String)
    createdAt = Column(DateTime, default=func.now())
    updatedAt = Column(DateTime, default=func.now(), onupdate=func.now())

    # relationships
    order = relationship("Order", back_populates="shipping")

# Create dim/fact table
class FactOrder(base):
    __tablename__ = "fct_order"

    id = Column(Integer, primary_key=True)
    orderId = Column(String, nullable=False)
    productId = Column(String, ForeignKey("dim_product.sku"), nullable=False)
    customerId = Column(Integer, ForeignKey("dim_customer.customerId"), nullable=False)
    shippingId = Column(Integer, ForeignKey("dim_shipping.shippingId"), nullable=False)
    dateId = Column(Integer, ForeignKey("dim_date.dateId"), nullable=False)
    quantity = Column(Integer)
    amount = Column(Float)
    createdAt = Column(DateTime, default=func.now())
    updatedAt = Column(DateTime, default=func.now(), onupdate=func.now())

    # relationships
    dim_customer = relationship("DimCustomer", back_populates="fct_order")
    dim_product = relationship("DimProduct", back_populates="fct_order")
    dim_shipping = relationship("DimShipping", back_populates="fct_order")
    dim_date = relationship("DimDate", back_populates="fct_order")

class DimShipping(base):
    __tablename__ = "dim_shipping"

    shippingId = Column(Integer, primary_key=True)
    ServiceLevel = Column(String)
    courierStatus = Column(String)
    shipping_city = Column(String)
    shipping_country = Column(String)
    createdAt = Column(DateTime, default=func.now())
    updatedAt = Column(DateTime, default=func.now(), onupdate=func.now())

    fct_order = relationship("FactOrder", back_populates="dim_shipping")

class DimCustomer(base):
    __tablename__ = "dim_customer"

    customerId = Column(Integer, primary_key=True)
    name = Column(String)
    email = Column(String)
    phone = Column(String)
    country = Column(String)
    city = Column(String)
    createdAt = Column(DateTime, default=func.now())
    updatedAt = Column(DateTime, default=func.now(), onupdate=func.now())

    fct_order = relationship("FactOrder", back_populates="dim_customer")

class DimProduct(base):
    __tablename__ = "dim_product"

    productId = Column(Integer, primary_key=True)
    sku = Column(String, unique=True)
    style = Column(String)
    category = Column(String)
    size = Column(String)
    asin = Column(String)
    createdAt = Column(DateTime, default=func.now())
    updatedAt = Column(DateTime, default=func.now(), onupdate=func.now())

    fct_order = relationship("FactOrder", back_populates="dim_product")

class DimDate(base):
    __tablename__ = "dim_date"

    dateId = Column(Integer, primary_key=True, autoincrement=True)
    day_of_week = Column(Integer)
    day_of_month = Column(Integer)
    day_of_year = Column(Integer)
    is_last_day_of_month = Column(Integer)
    is_weekend = Column(Integer)
    week_start_id = Column(Integer)
    week_end_id = Column(Integer)
    week_of_year = Column(Integer)
    month = Column(Integer)
    quarter = Column(String)
    year = Column(Integer)
    is_holiday = Column(Integer)
    unix_timestamp = Column(Integer)
    date = Column(DateTime)

    fct_order = relationship("FactOrder", back_populates="dim_date")



