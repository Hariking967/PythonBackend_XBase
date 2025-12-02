from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, DateTime
from sqlalchemy.dialects.postgresql import UUID
import uuid
from sqlalchemy.sql import func

Base = declarative_base()

class Folder(Base):
    __tablename__ = "folders"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # parent folder UUID (no foreign key restriction)
    parent_id = Column(UUID(as_uuid=True), nullable=False)


class File(Base):
    __tablename__ = "files"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # parent folder UUID (no foreign key restriction)
    parent_id = Column(UUID(as_uuid=True), nullable=False)
    bucket_url = Column(String, nullable=False)

class UserRoot(Base):
    __tablename__ = "user_root"

    # unique row id
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # user.id in drizzle is TEXT → so store user_id as TEXT in SQLAlchemy too
    user_id = Column(String, nullable=False, unique=True)

    # root folder UUID
    root = Column(UUID(as_uuid=True), nullable=False)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
