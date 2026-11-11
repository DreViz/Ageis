import os
import multiprocessing
import shutil
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import pytest
from sqlalchemy import create_engine, inspect, text
from ageis_migrations.__main__ import MigrationError, migrate
from runtime.db import Base as RuntimeBase, Run
from mock_identity.db import Base as IdentityBase, Grant
from mock_notice.app import Base as NoticeBase, Fault

URL = os.getenv('TEST_DATABASE_URL')
pytestmark = pytest.mark.skipif(not URL, reason='PostgreSQL TEST_DATABASE_URL required')


def bootstrap_process(schema):
    engine = create_engine(URL, connect_args={'options': f'-csearch_path={schema}'})
    try:
        migrate(engine, 'identity', schema=schema, startup=True)
    finally:
        engine.dispose()


@pytest.fixture
def database():
    schema = 'test_migration_' + uuid.uuid4().hex
    admin = create_engine(URL)
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(URL, connect_args={'options': f'-csearch_path={schema}'})
    yield engine, schema
    engine.dispose()
    with admin.begin() as conn:
        conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    admin.dispose()


@pytest.mark.parametrize('component,base,table', [
    ('runtime', RuntimeBase, 'runs'), ('identity', IdentityBase, 'operations'),
    ('notice', NoticeBase, 'synthetic_notices')])
def test_fresh_and_legacy_adoption_preserves_data(database, component, base, table):
    engine, schema = database
    base.metadata.create_all(engine)
    from sqlalchemy.orm import Session
    with Session(engine) as session, session.begin():
        if component == 'runtime':
            session.add(Run(id='legacy-run', state='PENDING', request={}, action={}, action_hash='hash',
                            idempotency_key='key', action_status='NOT_SENT', deadline=datetime.now(timezone.utc),
                            attempt_count=0, max_attempts=3, event_seq=0))
        elif component == 'identity':
            session.add(Grant(id='legacy-grant', tenant_id='tenant', user_id='user', workspace_id='workspace'))
        else:
            session.add(Fault(org_id='legacy-org', scenario='normal', calls=0))
    # An unrelated table in the shared schema must also be preserved.
    with engine.begin() as conn:
        conn.execute(text('CREATE TABLE preserved (value TEXT NOT NULL)'))
        conn.execute(text("INSERT INTO preserved VALUES ('keep')"))
    with pytest.raises(MigrationError, match='unversioned'):
        migrate(engine, component, schema=schema, startup=True)
    migrate(engine, component, schema=schema, adopt_legacy=True)
    migrate(engine, component, schema=schema, startup=True)
    migrate(engine, component, schema=schema)
    with engine.connect() as conn:
        assert conn.scalar(text('SELECT value FROM preserved')) == 'keep'
        assert conn.scalar(text(f'SELECT version_num FROM ageis_{component}_version')) == '0001'
        assert table in inspect(conn).get_table_names(schema=schema)
        if component == 'runtime':
            assert conn.scalar(text("SELECT state FROM runs WHERE id = 'legacy-run'")) == 'PENDING'
        elif component == 'identity':
            assert conn.scalar(text("SELECT workspace_id FROM grants WHERE id = 'legacy-grant'")) == 'workspace'
        else:
            assert conn.scalar(text("SELECT scenario FROM synthetic_notice_faults WHERE org_id = 'legacy-org'")) == 'normal'


@pytest.mark.parametrize('component,base,table', [
    ('runtime', RuntimeBase, 'runs'), ('identity', IdentityBase, 'operations'),
    ('notice', NoticeBase, 'synthetic_notices')])
def test_mismatched_legacy_schema_is_never_stamped(database, component, base, table):
    engine, schema = database
    base.metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(text(f'ALTER TABLE {table} ADD COLUMN unexpected TEXT'))
    with pytest.raises(MigrationError, match='differs'):
        migrate(engine, component, schema=schema, adopt_legacy=True)
    assert f'ageis_{component}_version' not in inspect(engine).get_table_names(schema=schema)


def test_shared_schema_bootstrap_is_serialized_and_idempotent(database):
    engine, schema = database
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda _: migrate(engine, 'runtime', schema=schema, startup=True), range(4)))
    with ProcessPoolExecutor(max_workers=2, mp_context=multiprocessing.get_context('spawn')) as pool:
        list(pool.map(bootstrap_process, [schema, schema]))
    migrate(engine, 'notice', schema=schema, startup=True)
    with engine.connect() as conn:
        tables = set(inspect(conn).get_table_names(schema=schema))
        assert {'runs', 'operations', 'synthetic_notices', 'ageis_runtime_version',
                'ageis_identity_version', 'ageis_notice_version'} <= tables


def test_unknown_revision_fails_closed(database):
    engine, schema = database
    migrate(engine, 'runtime', schema=schema, startup=True)
    with engine.begin() as conn:
        conn.execute(text("UPDATE ageis_runtime_version SET version_num = 'future'"))
    with pytest.raises(MigrationError, match='requires a migration'):
        migrate(engine, 'runtime', schema=schema, startup=True)


def test_versioned_upgrade_preserves_rows(database, tmp_path, monkeypatch):
    import ageis_migrations.__main__ as migrations
    engine, schema = database
    migration_root = tmp_path / 'migrations'
    shutil.copytree(migrations.ROOT, migration_root)
    monkeypatch.setattr(migrations, 'ROOT', migration_root)
    migrate(engine, 'identity', schema=schema)
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO grants (id, tenant_id, user_id, workspace_id) VALUES ('kept', 't', 'u', 'w')"))
    (migration_root / 'identity' / 'versions' / '0002_test.py').write_text(
        "from alembic import op\nimport sqlalchemy as sa\nrevision='0002'\ndown_revision='0001'\nbranch_labels=None\ndepends_on=None\ndef upgrade():\n    op.add_column('grants', sa.Column('note', sa.String(), nullable=True))\n", encoding='utf-8')
    with pytest.raises(MigrationError, match='requires a migration'):
        migrate(engine, 'identity', schema=schema, startup=True)
    migrate(engine, 'identity', schema=schema)
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT workspace_id FROM grants WHERE id='kept'")) == 'w'
        assert conn.scalar(text('SELECT version_num FROM ageis_identity_version')) == '0002'
        assert 'note' in {column['name'] for column in inspect(conn).get_columns('grants', schema=schema)}


def test_partial_legacy_schema_is_not_adopted(database):
    engine, schema = database
    with engine.begin() as conn:
        conn.execute(text('CREATE TABLE grants (id VARCHAR PRIMARY KEY)'))
    with pytest.raises(MigrationError, match='incomplete'):
        migrate(engine, 'identity', schema=schema, adopt_legacy=True)
    assert 'ageis_identity_version' not in inspect(engine).get_table_names(schema=schema)


def test_upgrade_cli_uses_requested_schema(database):
    engine, schema = database
    env = {**os.environ, 'DATABASE_URL': URL}
    result = subprocess.run([sys.executable, '-m', 'ageis_migrations', 'upgrade', 'identity', '--schema', schema],
                            env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert 'identity database is at migration head' in result.stdout
    assert 'ageis_identity_version' in inspect(engine).get_table_names(schema=schema)
