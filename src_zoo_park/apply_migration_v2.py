import asyncio
import sys
import os

# Add current directory to path to allow imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import text
from init_db import _engine

async def main():
    async with _engine.begin() as conn:
        print("Checking and applying migration for 'users' table...")
        
        # Add income_per_minute
        try:
            await conn.execute(text("ALTER TABLE users ADD COLUMN income_per_minute BIGINT DEFAULT 0;"))
            print("- Added column 'income_per_minute'")
        except Exception as e:
            if "Duplicate column name" in str(e) or "already exists" in str(e).lower():
                print("- Column 'income_per_minute' already exists")
            else:
                print(f"- Error adding 'income_per_minute': {e}")

        # Add last_income_at
        try:
            await conn.execute(text("ALTER TABLE users ADD COLUMN last_income_at DATETIME DEFAULT CURRENT_TIMESTAMP;"))
            print("- Added column 'last_income_at'")
        except Exception as e:
            if "Duplicate column name" in str(e) or "already exists" in str(e).lower():
                print("- Column 'last_income_at' already exists")
            else:
                print(f"- Error adding 'last_income_at': {e}")

        # Add Index
        try:
            await conn.execute(text("CREATE INDEX ix_users_income_per_minute ON users (income_per_minute);"))
            print("- Created index 'ix_users_income_per_minute'")
        except Exception as e:
            if "Duplicate key name" in str(e) or "already exists" in str(e).lower():
                print("- Index 'ix_users_income_per_minute' already exists")
            else:
                print(f"- Error creating index: {e}")

        print("Migration process finished!")

if __name__ == "__main__":
    asyncio.run(main())
