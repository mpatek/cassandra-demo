import datetime
import logging

from utils import (
    ignore_already_exists,
    item_from_text,
    item_to_text,
)

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


def get_last_event(event_type, session):
    query = """
    SELECT * FROM timeseries
    WHERE event_type = %s
    ORDER BY insertion_time DESC
    LIMIT 1
    """
    rows = list(session.execute(query, [event_type]))
    if rows:
        return item_from_text(rows[0].event), rows[0].insertion_time
    return None, None


def update_last_check_time(event_type, session):
    logger.info('Updating last check time for event: %s', event_type)
    _, insertion_time = get_last_event(event_type, session)
    query = """
    UPDATE timeseries
    SET
        last_check_time = %(now)s
    WHERE
        event_type = %(event_type)s
        AND
        insertion_time = %(insertion_time)s
    """
    session.execute(
        query,
        {
            'event_type': event_type,
            'insertion_time': insertion_time,
            'now': datetime.datetime.now(),
        }
    )


def insert_event(event_type, event, session):
    logger.info('Inserting event: %s: %s', event_type, event)
    query = """
    INSERT INTO timeseries (
        event_type,
        event,
        insertion_time,
        last_check_time
    )
    VALUES (
        %(event_type)s,
        %(event)s,
        %(now)s,
        %(now)s
    )
    """
    session.execute(
        query,
        {
            'event_type': event_type,
            'event': item_to_text(event),
            'now': datetime.datetime.now(),
        }
    )


def upsert_event(event_type, event, session):
    logger.info('Upserting event: %s: %s', event_type, event)
    existing_event, _ = get_last_event(event_type, session)
    if not existing_event or existing_event != event:
        insert_event(event_type, event, session)
    else:
        update_last_check_time(event_type, session)
