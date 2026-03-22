import asyncio
import os
import sys

# Ensure current working directory is in path
sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), "src_zoo_park"))

# Try importing from src_zoo_park
try:
    from src_zoo_park.init_db import async_session
    from src_zoo_park.db.models import Animal
except ImportError:
    from init_db import async_session
    from db.models import Animal

from sqlalchemy import select

async def main():
    try:
        async with async_session() as s:
            res = await s.execute(select(Animal.code_name).limit(100))
            codes = [a for a in res.scalars()]
            print(f"Count: {len(codes)}")
            print(codes)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
