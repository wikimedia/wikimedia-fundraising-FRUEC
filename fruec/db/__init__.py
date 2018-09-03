"""This module and its submodules provide database-related logic. No other code should
touch the database directly.
This module provides functions for database connection, and a cache for objects in the
database (used by the mapper submodules).
"""


import mysql.connector as mariadb
from . import ( log_file_mapper, cn_event_aggregator, lookup_on_unique_column_helper,
    lp_event_writer )


connection = None
_object_cache = {}


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


def get_cached_object( key ):
    return _object_cache.get( key, None )


def set_object_in_cache( key, obj ):
    _object_cache[ key ] = obj


def object_in_cache( key ):
    return key in _object_cache
