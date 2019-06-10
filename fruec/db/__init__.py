"""This module and its submodules provide database-related logic. No other code should
touch the database directly.
This module provides functions for the database connection.
"""


import mysql.connector as mariadb
from . import ( log_file_mapper, cn_event_aggregator, lp_event_writer,
    lookup_on_unique_column_helper, object_cache )


connection = None


def connect( user, password, host, database ):
    global connection

    if connection is not None:
        connection.close()
        raise RuntimeError( 'Attempt to connect to DB after connection already created.' )

    # TODO Do we need to ensure this closes on error?
    connection = mariadb.connect( user = user, password = password, host = host,
        database = database )

    return connection


def close():
    global connection

    if connection is None:
        raise RuntimeError( 'Attempt to close DB before connection was created.' )

    connection.close()
    connection = None
