from sqlalchemy import Column, Integer, Numeric
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import TypeDecorator


class BigInt(TypeDecorator):
    """Numeric(65,0) в БД, всегда int в Python."""
    impl = Numeric(precision=65, scale=0)
    cache_ok = True

    def process_result_value(self, value, dialect):
        return int(value) if value is not None else None


class Base(DeclarativeBase, AsyncAttrs):
    
    idpk: Mapped[int] = mapped_column(Integer, primary_key=True)

    repr_cols_num = 3
    repr_cols = ()
    
    def as_dict(self):
        return {column.name: getattr(self, column.name) for column in self.__table__.columns}
    def __repr__(self):
        cols = [
            f"{col}={getattr(self, col)}"
            for idx, col in enumerate(self.__table__.columns.keys())
            if col in self.repr_cols or idx < self.repr_cols_num
        ]
        return f"<{self.__class__.__name__} {', '.join(cols)}>"