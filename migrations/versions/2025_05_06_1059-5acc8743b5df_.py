"""empty message

Revision ID: 5acc8743b5df
Revises: bf3c49094c43
Create Date: 2025-05-06 10:59:06.224992

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "5acc8743b5df"
down_revision: Union[str, None] = "bf3c49094c43"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "user_types",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("code", sa.String(), nullable=False),
        sa.Column("description", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.add_column("users", sa.Column("type_id", sa.Integer(), nullable=True))
    op.create_foreign_key(None, "users", "user_types", ["type_id"], ["id"])


def downgrade() -> None:
    op.drop_constraint(None, "users", type_="foreignkey")
    op.drop_column("users", "type_id")
    op.drop_table("user_types")
