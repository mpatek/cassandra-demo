import hashlib
import logging

from cassandra.cluster import Cluster

import warehouse
import items
import timeseries
from utils import item_to_text

logging.basicConfig(level=logging.INFO)


def get_hash(identifier):
    return hashlib.sha224(
        item_to_text(identifier).encode()
    ).hexdigest()


def get_item_id(item):
    return get_hash((item['id'], 'post'))


def get_event_type(event):
    return get_hash((event['id'], 'post-pageviews'))


def main():
    cluster = Cluster()
    session = cluster.connect()

    keyspace = 'warehouse'
    warehouse.create_keyspace(keyspace, session)
    session.set_keyspace(keyspace)

    items.create_table(session)
    item = {
        'id': 1234,
        'title': 'Test Item',
    }
    item_id = get_item_id(item)

    # new item, insert
    items.upsert_item(item_id, item, session)

    # unchanged item, update check time
    items.upsert_item(item_id, item, session)

    # changed item, update
    timeseries.create_table(session)
    item['title'] = 'New title'
    items.upsert_item(item_id, item, session)

    # fetch item
    item_from_db = items.get_item(item_id, session)
    assert item_from_db == item

    event = {
        'id': 1234,
        'pageviews': 100,
    }
    event_type = get_event_type(event)

    # new event, insert
    timeseries.upsert_event(event_type, event, session)

    # unchanged event, update check time
    timeseries.upsert_event(event_type, event, session)

    # changed event, insert
    event['pageviews'] = 120
    timeseries.upsert_event(event_type, event, session)

    # fetch latest event
    event_from_db, insert_time = timeseries.get_last_event(event_type, session)
    assert event_from_db == event

    warehouse.drop_keyspace(keyspace, session)


if __name__ == '__main__':
    main()
