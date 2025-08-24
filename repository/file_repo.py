from sqlalchemy.ext.asyncio import AsyncSession
from models.File import File

async def save_file(db: AsyncSession, file: File, user_id: str) -> File:
    """
    Save a file to the database.
    
    """
    db.add(
        File(file_size)
    )
    await db.commit()
    await db.refresh(file)
    return file