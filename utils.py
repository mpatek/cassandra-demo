import hashlib
import json

import cassandra


def ignore_already_exists(query, session):
    try:
        session.execute(query)
    except cassandra.AlreadyExists:
        pass


def item_to_text(item):
    return json.dumps(item)


def item_from_text(text):
    return json.loads(text)


def get_hash(identifier):
    return hashlib.sha224(
        item_to_text(identifier).encode()
    ).hexdigest()
