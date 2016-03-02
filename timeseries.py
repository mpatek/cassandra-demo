import logging

from utils import ignore_already_exists

logger = logging.getLogger(__name__)


def create_table(session):
    logger.info('Creating timeseries table.')
    query = """
    CREATE TABLE timeseries (
        event_type TEXT,
        insertion_time TIMESTAMP,
        last_check_time TIMESTAMP,
        event TEXT,
        PRIMARY KEY (event_type, insertion_time)
    )
    """
    ignore_already_exists(query, session)
