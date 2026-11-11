"""Alembic environment configured with a caller-owned connection."""
from alembic import context

config = context.config
connection = config.attributes['connection']
context.configure(connection=connection, version_table=config.attributes['version_table'],
                  transactional_ddl=True)
with context.begin_transaction():
    context.run_migrations()
