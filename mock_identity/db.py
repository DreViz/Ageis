"""Mock service persistence. Never use this schema for real identity data."""
from datetime import datetime, timezone
from sqlalchemy import DateTime, String, UniqueConstraint, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


def now():
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Operation(Base):
    __tablename__ = "operations"
    __table_args__ = (UniqueConstraint("tenant_id", "operation_type", "key"),)
    id: Mapped[str] = mapped_column(String, primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String, nullable=False)
    operation_type: Mapped[str] = mapped_column(String, nullable=False)
    key: Mapped[str] = mapped_column(String, nullable=False)
    request_hash: Mapped[str] = mapped_column(String, nullable=False)
    action_id: Mapped[str] = mapped_column(String, nullable=False)
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    workspace_id: Mapped[str] = mapped_column(String, nullable=False)
    committed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now, nullable=False)


class Grant(Base):
    __tablename__ = "grants"
    __table_args__ = (UniqueConstraint("tenant_id", "user_id", "workspace_id"),)
    id: Mapped[str] = mapped_column(String, primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String, nullable=False)
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    workspace_id: Mapped[str] = mapped_column(String, nullable=False)


class Attempt(Base):
    __tablename__ = "attempts"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String, nullable=False)
    key: Mapped[str] = mapped_column(String, nullable=False)
    request_hash: Mapped[str] = mapped_column(String, nullable=False)
    action_id: Mapped[str] = mapped_column(String, nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now, nullable=False)
    outcome: Mapped[str] = mapped_column(String, default="received", nullable=False)


def make_database(url):
    kwargs = {"check_same_thread": False, "timeout": 30} if url.startswith("sqlite") else {}
    engine = create_engine(url, connect_args=kwargs)
    Base.metadata.create_all(engine)
    return engine, sessionmaker(engine, expire_on_commit=False)
