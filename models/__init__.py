from sqlalchemy.orm import declarative_base

Base = declarative_base()

# import semua model agar dikenali SQLAlchemy
from .User import User
from .File import File
