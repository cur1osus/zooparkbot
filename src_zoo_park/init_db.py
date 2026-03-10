from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

import config


ENGINE_OPTIONS = {
    "pool_pre_ping": True,
}


_engine = create_async_engine(config.DATABASE_URL, **ENGINE_OPTIONS)
_engine_for_func = _engine
_sessionmaker = async_sessionmaker(_engine, expire_on_commit=False)
_sessionmaker_for_func = _sessionmaker
