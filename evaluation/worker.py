"""Worker for durable synthetic evaluation jobs."""
import logging
import os
import time
import uuid
from runtime.db import make_database
from .jobs import JobService


def main():
    logging.basicConfig(level=logging.INFO)
    engine, sessions = make_database(os.environ['RUNTIME_DATABASE_URL'])
    service = JobService(sessions, {'access': os.environ['MOCK_IDENTITY_URL'], 'notice': os.environ['MOCK_NOTICE_URL']})
    owner = str(uuid.uuid4())
    try:
        while True:
            try:
                lease = service.claim(owner)
                if lease:
                    service.process(lease, os.environ['RUNTIME_DATABASE_URL'])
                else:
                    time.sleep(1)
            except Exception:
                logging.exception('Evaluation worker failed; the lease will expire')
                time.sleep(1)
    finally:
        engine.dispose()


if __name__ == '__main__':
    main()
