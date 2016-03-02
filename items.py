import datetime
import logging

from utils import (
    ignore_already_exists,
    item_from_text,
    item_to_text,
)

logger = logging.getLogger(__name__)


def create_table(session):
    logger.info('Creating items table.')
    query = """
    CREATE TABLE items (
        item_id TEXT PRIMARY KEY,
        insertion_time TIMESTAMP,
        last_modified_time TIMESTAMP,
        last_check_time TIMESTAMP,
        item TEXT
    )
    """
    ignore_already_exists(query, session)


def insert_item(item_id, item, session):
    logger.info('Inserting item: %s: %s', item_id, item)
    query = """
    INSERT INTO items (
        item_id,
        item,
        insertion_time,
        last_modified_time,
        last_check_time
    )
    VALUES (
        %(item_id)s,
        %(item)s,
        %(now)s,
        %(now)s,
        %(now)s
    )
    """
    session.execute(
        query,
        {
            'item_id': item_id,
            'item': item_to_text(item),
            'now': datetime.datetime.now(),
        }
    )


def update_last_check_time(item_id, session):
    logger.info('Updating last check time for item: %s', item_id)
    query = """
    UPDATE items
    SET
        last_check_time = %(now)s
    WHERE item_id = %(item_id)s
    """
    session.execute(
        query,
        {
            'item_id': item_id,
            'now': datetime.datetime.now()
        }
    )


def update_item(item_id, item, session):
    logger.info('Updating item: %s: %s', item_id, item)
    query = """
    UPDATE items
    SET
        item = %(item)s,
        last_modified_time = %(now)s,
        last_check_time = %(now)s
    WHERE item_id = %(item_id)s
    """
    session.execute(
        query,
        {
            'item_id': item_id,
            'item': item_to_text(item),
            'now': datetime.datetime.now(),
        },
    )


def upsert_item(item_id, item, session):
    query = """
    SELECT * FROM items WHERE item_id=%s
    """
    logger.info('Upserting item: %s, %s', item_id, item)
    rows = list(session.execute(query, [item_id]))
    if rows:
        # Do an update
        existing_item = item_from_text(rows[0].item)
        if existing_item == item:
            update_last_check_time(item_id, session)
        else:
            update_item(item_id, item, session)
    else:
        # Do an insert
        insert_item(item_id, item, session)
