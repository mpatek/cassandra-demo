import logging

from cassandra.cluster import Cluster

import warehouse
import items
from utils import get_hash

logging.basicConfig(level=logging.INFO)


def get_item_id(item):
    return get_hash((item['id'], 'post'))


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
    item['title'] = 'New title'
    items.upsert_item(item_id, item, session)

    warehouse.drop_keyspace(keyspace, session)


if __name__ == '__main__':
    main()
