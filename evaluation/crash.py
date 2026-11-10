"""Bounded process-crash harness for isolated synthetic workflow fixtures."""
import multiprocessing
from datetime import timedelta

import httpx

from runtime.db import Job, make_database
from runtime.service import LeaseLost, Runtime, db_now
from runtime.worker import process


class CrashBoundary:
    def __init__(self, boundary, channel, point):
        self.boundary, self.channel, self.point = boundary, channel, point

    def post(self, path, **kwargs):
        if self.point == 'process-crash-before-send':
            self._pause('before_send')
        response = self.boundary.post(path, **kwargs)
        response.raise_for_status()
        if self.point == 'process-crash-after-commit':
            self._pause('after_commit')
        return response

    def get(self, path, **kwargs):
        return self.boundary.get(path, **kwargs)

    def _pause(self, boundary):
        self.channel.send(('boundary', boundary))
        # Do not leave an orphan worker blocked forever if the harness dies.
        if self.channel.poll(20):
            self.channel.recv()
        raise RuntimeError('Crash harness did not stop the worker')


def _worker(channel, database_url, schema, mock_url, workflow, scope, point):
    engine = None
    try:
        engine, sessions = make_database(database_url, schema=schema)
        runtime = Runtime(sessions)
        lease = runtime.claim('crash-original' if point else 'crash-replacement')
        if lease is None:
            raise RuntimeError('No job available to worker')
        channel.send(('lease', lease))
        with httpx.Client(base_url=mock_url, timeout=5) as client:
            process(runtime, lease, CrashBoundary(client, channel, point) if point else client)
        channel.send(('finished', True))
    except Exception as error:
        try:
            channel.send(('error', type(error).__name__ + (f':{error.response.status_code}' if isinstance(error, httpx.HTTPStatusError) else '')))
        except (BrokenPipeError, EOFError, OSError):
            pass
    finally:
        if engine is not None:
            engine.dispose()
        channel.close()


def _receive(channel, expected):
    if not channel.poll(20):
        raise RuntimeError('Worker did not reach the expected boundary')
    try:
        kind, value = channel.recv()
    except EOFError as error:
        raise RuntimeError('Worker exited before the expected boundary') from error
    if kind == 'error':
        raise RuntimeError(f'Worker failed before the expected boundary ({value})')
    if kind != expected:
        raise RuntimeError('Worker failed before the expected boundary')
    return value


def run_process_crash(database_url, sessions, schema, mock_url, workflow, scope, profile):
    context = multiprocessing.get_context('spawn')
    workers = []
    channels = []

    def start(point):
        parent, child = context.Pipe()
        worker = context.Process(target=_worker, args=(child, database_url, schema, mock_url, workflow, scope, point))
        worker.start()
        child.close()
        workers.append(worker)
        channels.append(parent)
        return worker, parent

    try:
        original, channel = start(profile)
        old = _receive(channel, 'lease')
        boundary = _receive(channel, 'boundary')
        expected = 'before_send' if profile == 'process-crash-before-send' else 'after_commit'
        if boundary != expected:
            raise RuntimeError('Worker reached the wrong boundary')
        original.kill()
        original.join(10)
        if original.is_alive() or original.exitcode in (None, 0):
            raise RuntimeError('Original worker did not terminate')
        # A real crash leaves the lease in place; advance its expiry in the isolated
        # fixture so tests do not wait for the production lease timeout.
        with sessions.begin() as session:
            job = session.get(Job, old[0])
            if job is None or job.lease_owner != old[1] or job.fence != old[2]:
                raise RuntimeError('Original worker lease changed unexpectedly')
            job.lease_expires_at = db_now(session) - timedelta(seconds=1)
        replacement, replacement_channel = start(None)
        new = _receive(replacement_channel, 'lease')
        try:
            Runtime(sessions).finish(old, None)
            stale_fenced = False
        except LeaseLost:
            stale_fenced = True
        _receive(replacement_channel, 'finished')
        replacement.join(10)
        if replacement.is_alive() or replacement.exitcode != 0:
            raise RuntimeError('Replacement worker did not finish cleanly')
        return {'execution': 'killed_worker_process', 'boundary': profile,
                'original_pid': original.pid, 'replacement_pid': replacement.pid,
                'original_exitcode': original.exitcode, 'replacement_exitcode': replacement.exitcode,
                'lease_expiry': 'advanced_in_fixture', 'old_fence': old[2],
                'replacement_fence': new[2], 'stale_completion_fenced': stale_fenced}
    finally:
        for worker in workers:
            if worker.is_alive():
                worker.kill()
            worker.join(10)
        for channel in channels:
            channel.close()
