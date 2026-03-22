import asyncio
import sys
import os
sys.path.append(os.path.join(os.getcwd(), "src_zoo_park"))

from sqlalchemy import text
from init_db import _engine

async def main():
    async with _engine.begin() as conn:
        print("Cleaning up old alembic version table...")
        await conn.execute(text("DROP TABLE IF EXISTS alembic_version;"))
        print("Done! Now run 'alembic stamp head' to initialize properly.")

if __name__ == "__main__":
    asyncio.run(main())
