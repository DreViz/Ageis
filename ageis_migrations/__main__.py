"""Component-local migration management and conservative legacy adoption."""
import argparse
import os
import re
import threading
import uuid
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text

ROOT = Path(__file__).resolve().parent
COMPONENTS = ('runtime', 'identity', 'notice')
# Alembic's operation and environment proxies are process-global.
_MIGRATION_LOCK = threading.RLock()
VERSION_TABLES = {name: f'ageis_{name}_version' for name in COMPONENTS}
# Tables owned by the frozen baseline. Other components may share the same schema.
TABLES = {
    'runtime': {'runs', 'jobs', 'approvals', 'events', 'model_sessions', 'evaluation_runs',
                'evaluation_scenarios', 'evaluation_comparisons', 'evaluation_decisions',
                'evaluation_jobs', 'evaluation_job_events'},
    'identity': {'operations', 'grants', 'attempts'},
    'notice': {'synthetic_notices', 'synthetic_notice_attempts', 'synthetic_notice_faults'},
}


class MigrationError(RuntimeError):
    pass


def _config(connection, component):
    config = Config()
    config.set_main_option('script_location', str(ROOT))
    config.set_main_option('path_separator', 'os')
    config.set_main_option('version_locations', str(ROOT / component / 'versions'))
    config.attributes.update(connection=connection, version_table=VERSION_TABLES[component])
    return config


def _schema_name(connection, schema):
    if connection.dialect.name == 'sqlite':
        return None
    name = schema or connection.scalar(text('SELECT current_schema()'))
    if not name or not re.fullmatch(r'[a-z][a-z0-9_]{0,62}', name):
        raise MigrationError('Invalid or missing PostgreSQL schema')
    return name


def _version(connection, component, schema):
    inspector = inspect(connection)
    if VERSION_TABLES[component] not in inspector.get_table_names(schema=schema):
        return None
    quoted = connection.dialect.identifier_preparer.quote(VERSION_TABLES[component])
    rows = connection.execute(text(f'SELECT version_num FROM {quoted}')).scalars().all()
    if len(rows) != 1:
        raise MigrationError(f'{component} migration version table is invalid')
    return rows[0]


def _signature(connection, schema, tables):
    inspector = inspect(connection)
    signature = {}
    for table in sorted(tables):
        columns = inspector.get_columns(table, schema=schema)
        signature[table] = {
            'columns': [(c['name'], str(c['type'].compile(dialect=connection.dialect)), c['nullable'], c.get('default')) for c in columns],
            'pk': tuple(inspector.get_pk_constraint(table, schema=schema)['constrained_columns']),
            'unique': sorted(tuple(c['column_names']) for c in inspector.get_unique_constraints(table, schema=schema)),
            'foreign': sorted((tuple(c['constrained_columns']), c['referred_table'], tuple(c['referred_columns']), c.get('referred_schema') not in (None, schema),
                               tuple(sorted((c.get('options') or {}).items()))) for c in inspector.get_foreign_keys(table, schema=schema)),
            'checks': sorted(c['sqltext'] for c in inspector.get_check_constraints(table, schema=schema)),
            'indexes': sorted((tuple(i['column_names']), i['unique']) for i in inspector.get_indexes(table, schema=schema)
                              if not i.get('duplicates_constraint')),
        }
    return signature


def _validate_legacy(connection, component, schema):
    if connection.dialect.name != 'postgresql':
        raise MigrationError('Legacy adoption is supported only for PostgreSQL')
    existing = set(inspect(connection).get_table_names(schema=schema)) & TABLES[component]
    if existing != TABLES[component]:
        raise MigrationError(f'{component} legacy schema is incomplete; no migration was applied')
    # Compare with an actual frozen baseline created inside this transaction.
    # The random, validated schema is dropped only after its identity is verified.
    reference = 'ageis_migration_ref_' + uuid.uuid4().hex
    preparer = connection.dialect.identifier_preparer
    quoted_reference = preparer.quote(reference)
    original_search_path = connection.scalar(text('SHOW search_path'))
    connection.execute(text(f'CREATE SCHEMA {quoted_reference}'))
    connection.execute(text(f'SET LOCAL search_path TO {quoted_reference}'))
    command.upgrade(_config(connection, component), '0001')
    expected = _signature(connection, reference, TABLES[component])
    actual = _signature(connection, schema, TABLES[component])
    connection.execute(text('SELECT set_config(\'search_path\', :path, true)'), {'path': original_search_path})
    if expected != actual:
        raise MigrationError(f'{component} legacy schema differs from the baseline; no migration was applied')
    if not re.fullmatch(r'ageis_migration_ref_[0-9a-f]{32}', reference):
        raise MigrationError('Invalid temporary schema')
    connection.execute(text(f'DROP SCHEMA {quoted_reference} CASCADE'))


def migrate(engine, component, *, schema=None, adopt_legacy=False, startup=False):
    with _MIGRATION_LOCK:
        return _migrate(engine, component, schema=schema, adopt_legacy=adopt_legacy, startup=startup)


def _migrate(engine, component, *, schema=None, adopt_legacy=False, startup=False):
    if component not in COMPONENTS:
        raise ValueError('Unknown migration component')
    with engine.begin() as connection:
        name = _schema_name(connection, schema)
        if connection.dialect.name == 'postgresql':
            # Serialize startup and manual upgrades for this component/schema.
            connection.execute(text('SELECT pg_advisory_xact_lock(hashtext(:name))'),
                               {'name': f'ageis-migrations:{name}:{component}'})
            if schema:
                quoted = connection.dialect.identifier_preparer.quote(name)
                connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS {quoted}'))
                connection.execute(text(f'SET LOCAL search_path TO {quoted}'))
        version = _version(connection, component, name)
        config = _config(connection, component)
        if version is not None:
            if startup:
                from alembic.script import ScriptDirectory
                if version != ScriptDirectory.from_config(config).get_current_head():
                    raise MigrationError(f'{component} database requires a migration; run the upgrade command')
            else:
                command.upgrade(config, 'head')
            return
        tables = set(inspect(connection).get_table_names(schema=name)) & TABLES[component]
        if tables:
            if not adopt_legacy:
                raise MigrationError(f'{component} database is unversioned; run upgrade with --adopt-legacy after backing up')
            _validate_legacy(connection, component, name)
            command.stamp(config, '0001')
            command.upgrade(config, 'head')
        else:
            command.upgrade(config, 'head')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['upgrade'])
    parser.add_argument('component', choices=COMPONENTS)
    parser.add_argument('--schema', help='PostgreSQL schema (runtime defaults to ageis_runtime)')
    parser.add_argument('--adopt-legacy', action='store_true')
    args = parser.parse_args()
    env = 'RUNTIME_DATABASE_URL' if args.component == 'runtime' else 'DATABASE_URL'
    url = os.environ[env]
    schema = args.schema or ('ageis_runtime' if args.component == 'runtime' else None)
    engine = create_engine(url)
    try:
        migrate(engine, args.component, schema=schema, adopt_legacy=args.adopt_legacy)
    finally:
        engine.dispose()
    print(f'{args.component} database is at migration head')


if __name__ == '__main__':
    main()
