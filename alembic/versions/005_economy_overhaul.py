"""economy overhaul: maintenance cost, clan specialization, sick animals

Revision ID: 005
Revises: 004
Create Date: 2026-03-24

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '005'
down_revision: Union[str, None] = '004'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Add maintenance_per_minute to users
    op.add_column(
        'users',
        sa.Column('maintenance_per_minute', sa.Numeric(precision=65, scale=0),
                  nullable=False, server_default='0'),
    )

    # 2. Add specialization to unity
    op.add_column(
        'unity',
        sa.Column('specialization', sa.String(length=32), nullable=True),
    )

    # 3. Create sick_animal_events table
    op.create_table(
        'sick_animal_events',
        sa.Column('idpk', sa.Integer(), primary_key=True),
        sa.Column('idpk_user', sa.Integer(), nullable=False, index=True),
        sa.Column('animal_code_name', sa.String(length=64), nullable=False),
        sa.Column('sick_since', sa.DateTime(), nullable=False),
        sa.Column('deadline', sa.DateTime(), nullable=False, index=True),
        sa.Column('is_cured', sa.Boolean(), nullable=False, server_default='0', index=True),
        sa.Column('cure_cost', sa.Numeric(precision=65, scale=0), nullable=False, server_default='0'),
    )
    op.create_index(
        'ix_sick_animal_events_user_active',
        'sick_animal_events',
        ['idpk_user', 'is_cured'],
    )


def downgrade() -> None:
    op.drop_table('sick_animal_events')
    op.drop_column('unity', 'specialization')
    op.drop_column('users', 'maintenance_per_minute')
