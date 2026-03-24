"""change game value columns to Numeric(65,0)

Revision ID: 004
Revises: 003
Create Date: 2026-03-24

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '004'
down_revision: Union[str, None] = '003'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_NUMERIC = sa.Numeric(precision=65, scale=0)


def upgrade() -> None:
    op.alter_column('animals', 'price',
                    existing_type=sa.BigInteger(), type_=_NUMERIC, existing_nullable=False)
    op.alter_column('animals', 'income',
                    existing_type=sa.BigInteger(), type_=_NUMERIC, existing_nullable=False)
    op.alter_column('user_aviary_states', 'current_price',
                    existing_type=sa.BigInteger(), type_=_NUMERIC,
                    existing_nullable=False, existing_server_default='0')
    op.alter_column('transfer_money', 'one_piece_sum',
                    existing_type=sa.BigInteger(), type_=_NUMERIC, existing_nullable=False)
    op.alter_column('values', 'value_int',
                    existing_type=sa.BigInteger(), type_=_NUMERIC,
                    existing_nullable=False, existing_server_default='0')


def downgrade() -> None:
    op.alter_column('values', 'value_int',
                    existing_type=_NUMERIC, type_=sa.BigInteger(),
                    existing_nullable=False, existing_server_default='0')
    op.alter_column('transfer_money', 'one_piece_sum',
                    existing_type=_NUMERIC, type_=sa.BigInteger(), existing_nullable=False)
    op.alter_column('user_aviary_states', 'current_price',
                    existing_type=_NUMERIC, type_=sa.BigInteger(),
                    existing_nullable=False, existing_server_default='0')
    op.alter_column('animals', 'income',
                    existing_type=_NUMERIC, type_=sa.BigInteger(), existing_nullable=False)
    op.alter_column('animals', 'price',
                    existing_type=_NUMERIC, type_=sa.BigInteger(), existing_nullable=False)
