"""add income caching fields

Revision ID: 002
Revises: 001
Create Date: 2026-03-23

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '002'
down_revision: Union[str, None] = '001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Use batch_alter_table for SQLite compatibility if needed, 
    # but for MySQL/Postgres direct add_column is fine.
    # We use 'if_not_exists' logic conceptually, but Alembic usually 
    # expects a clean slate. Since we already added them manually, 
    # we will use 'stamp' later.
    op.add_column('users', sa.Column('income_per_minute', sa.BigInteger(), nullable=False, server_default='0'))
    op.add_column('users', sa.Column('last_income_at', sa.DateTime(), nullable=False, server_default=sa.func.now()))
    op.create_index('ix_users_income_per_minute', 'users', ['income_per_minute'])


def downgrade() -> None:
    op.drop_index('ix_users_income_per_minute', table_name='users')
    op.drop_column('users', 'last_income_at')
    op.drop_column('users', 'income_per_minute')
