"""create documents table

Revision ID: 001
Revises: 
Create Date: 2026-06-20 00:00:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '001'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    # Create Status ENUM
    status_enum = postgresql.ENUM('uploaded', 'chunking', 'embedding', 'ready', 'failed', name='documentstatus')
    status_enum.create(op.get_bind(), checkfirst=True)
    
    # Create Documents Table
    op.create_table(
        'documents',
        sa.Column('id', sa.dialects.postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('filename', sa.String(), nullable=False),
        sa.Column('namespace', sa.String(), nullable=False, server_default='default'),
        sa.Column('chunk_count', sa.Integer(), nullable=True),
        sa.Column('chunking_strategy', sa.String(), nullable=True),
        sa.Column('embedding_provider', sa.String(), nullable=True),
        sa.Column('status', sa.Enum('uploaded', 'chunking', 'embedding', 'ready', 'failed', name='documentstatus'), nullable=False),
        sa.Column('uploaded_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.PrimaryKeyConstraint('id')
    )

def downgrade() -> None:
    op.drop_table('documents')
    
    status_enum = postgresql.ENUM('uploaded', 'chunking', 'embedding', 'ready', 'failed', name='documentstatus')
    status_enum.drop(op.get_bind(), checkfirst=True)
