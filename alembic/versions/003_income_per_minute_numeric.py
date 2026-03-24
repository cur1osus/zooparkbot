"""change income_per_minute to Numeric(65,0)

Revision ID: 003
Revises: 002
Create Date: 2026-03-24

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '003'
down_revision: Union[str, None] = '002'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        'users',
        'income_per_minute',
        existing_type=sa.BigInteger(),
        type_=sa.Numeric(precision=65, scale=0),
        existing_nullable=False,
        existing_server_default='0',
    )


def downgrade() -> None:
    op.alter_column(
        'users',
        'income_per_minute',
        existing_type=sa.Numeric(precision=65, scale=0),
        type_=sa.BigInteger(),
        existing_nullable=False,
        existing_server_default='0',
    )
