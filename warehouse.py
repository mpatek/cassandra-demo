import logging

from utils import ignore_already_exists

logger = logging.getLogger(__name__)


def create_keyspace(keyspace, session):
    logger.info('Creating keyspace: {}'.format(keyspace))
    query = """
    CREATE KEYSPACE {}
    WITH REPLICATION = {{
        'class': 'SimpleStrategy',
        'replication_factor': 3
    }}
    """.format(keyspace)
    ignore_already_exists(query, session)


def drop_keyspace(keyspace, session):
    logger.info('Dropping keyspace: {}'.format(keyspace))
    query = 'DROP KEYSPACE {}'.format(keyspace)
    session.execute(query)
