"""empty message

Revision ID: 9dc7c26395d9
Revises: b489bd0d1bc2
Create Date: 2025-05-10 13:23:04.103507

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "9dc7c26395d9"
down_revision: Union[str, None] = "b489bd0d1bc2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("users", sa.Column("created_by_id", sa.Integer(), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("users", "created_by_id")
    # ### end Alembic commands ###
