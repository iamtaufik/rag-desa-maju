import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from models import Base
from settings import DATABASE_URL

# contoh: "postgresql+asyncpg://user:pass@localhost/db"
engine = create_async_engine(DATABASE_URL, echo=True)

async def run_migration():
    print(f"\n\n{DATABASE_URL}\n\n")
    print("Running migration...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("Migration done!")

if __name__ == "__main__":
    asyncio.run(run_migration())
