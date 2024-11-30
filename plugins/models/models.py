import enum

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    PrimaryKeyConstraint,
    String,
    func,
)
from sqlalchemy.orm import declarative_base, relationship

base = declarative_base()


class Product(base):
    __tablename__ = "product"
    __table_args__ = (PrimaryKeyConstraint("sku", name="sku"),)

    sku = Column(String, unique=True)
    style = Column(String)
    category = Column(String)
    size = Column(String)
    asin = Column(String, unique=True)
    createdAt = Column(DateTime, default=func.now())
    updatedAt = Column(DateTime, default=func.now(), onupdate=func.now())


class Customer(base):
    __tablename__ = "customer"
    __table_args__ = (PrimaryKeyConstraint("customerId", name="customerId"),)

    customerId = Column(String, unique=True)
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


class OrderStatus(enum.Enum):
    SHIPPED = "shipped"
    SHIPPED_DELIVERED_TO_BUYER = "shipped-delivered-to-buyer"
    SHIPPED_RETURNED_TO_SELLER = "shipped-returned-to-seller"
    SHIPPED_PICKED_UP = "shipped-picked-up"
    CANCELLED = "cancelled"


class OrderFullfillment(enum.Enum):
    AMAZON = "amazon"
    MERCHANT = "merchant"


class OrderSalesChannel(enum.Enum):
    AMAZON = "amazon"
    NOT_AMAZON = "not-amazon"


class Order(base):
    __tablename__ = "order"
    __table_args__ = (PrimaryKeyConstraint("orderId", name="orderId"),)

    orderId = Column(String, unique=True)
    date = Column(DateTime)
    B2B = Column(Boolean)
    status = Column(Enum(OrderStatus), nullable=False)
    fullfillment = Column(Enum(OrderFullfillment), nullable=False)
    orderChannel = Column(Enum(OrderSalesChannel), nullable=False)
    promotionIds = Column(String)
    createdAt = Column(DateTime, default=func.now())
    updatedAt = Column(DateTime, default=func.now(), onupdate=func.now())

    # relationships
    customerId = Column(
        Integer, ForeignKey("customer.customerId"), nullable=False
    )
    customer = relationship("Customer", back_populates="order")


class OrderDetail(base):
    __tablename__ = "order_detail"
    __table_args__ = (
        PrimaryKeyConstraint("orderId", "sku", name="orderId_sku"),
    )

    orderId = Column(String, ForeignKey("order.orderId"), nullable=False)
    sku = Column(String, ForeignKey("product.sku"), nullable=False)
    quantity = Column(Integer, nullable=False)
    amount = Column(Integer, nullable=False)
    currency = Column(String, nullable=False, default="INR")
    createdAt = Column(DateTime, default=func.now())
    updatedAt = Column(DateTime, default=func.now(), onupdate=func.now())

    # relationships
    order = relationship("Order", back_populates="order_detail")
    product = relationship("Product", back_populates="order_detail")


class ShippingServiceLevel(enum.Enum):
    STANDARD = "standard"
    EXPEDITED = "expedited"


class ShippingCourierStatus(enum.Enum):
    SHIPPED = "shipped"
    UNSHIPPED = "unshipped"
    CANCELLED = "cancelled"


class Shipping(base):
    __tablename__ = "shipping"
    __table_args__ = (PrimaryKeyConstraint("orderId", name="orderId"),)

    orderId = Column(String, ForeignKey("order.orderId"), nullable=False)
    shipServiceLevel = Column(Enum(ShippingServiceLevel), nullable=False)
    courierStatus = Column(Enum(ShippingCourierStatus), nullable=False)
    city = Column(String)
    state = Column(String)
    postalCode = Column(String)
    country = Column(String)
    createdAt = Column(DateTime, default=func.now())
    updatedAt = Column(DateTime, default=func.now(), onupdate=func.now())

    # relationships
    order = relationship("Order", back_populates="shipping")
