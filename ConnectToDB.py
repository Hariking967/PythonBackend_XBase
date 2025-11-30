from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
import ssl

# SSL for Neon
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

DATABASE_URL = "postgresql+asyncpg://neondb_owner:npg_WuVxFZbi42nH@ep-steep-band-adp33aft-pooler.c-2.us-east-1.aws.neon.tech/neondb"

# 1. Create ASYNC engine
engine = create_async_engine(
    DATABASE_URL,
    echo=True,
    connect_args={"ssl": ssl_context}
)

# 2. Create ASYNC session factory
AsyncSessionLocal = sessionmaker(
    bind=engine,
    expire_on_commit=False,
    class_=AsyncSession
)

# 3. Dependency / getter
async def get_session():
    async with AsyncSessionLocal() as session:
        yield session
