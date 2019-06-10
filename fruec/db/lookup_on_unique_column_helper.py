"""Helper module for db operations involving objects identified by a column with unique
values. Used by mappers for language, project and country.
"""


import mysql.connector as mariadb

from fruec import db


def get_or_new( unique_column_val, cache_key, get_sql, insert_sql, new_obj_callback ):
    """Return an object for the provided unique column value. If there is already a
    corresponding row for this object in the database, it will be retrieved from there
    (or from the object cache). If not, a corresponding row will be inserted.

    Note: This function only works for objects with exactly two attributes: one unique
    field for lookup (such as language code or project identifier) and db_id.

    :param str unique_column_val: Value of the unique column that identifies the object.
    :param str cache_key: Cache key for the object.
    :param str get_sql: SQL to retrieve object data from the database.
    :param str insert_sql: SQL to insert a row for this object in the database.
    :param function new_obj_callback: Function that returns a new object of the
        appropriate type, with the unique field's value already set.
    """

    # Check the object cache. If a corresponding object is there, just return that.
    obj = db.object_cache.get_obj( cache_key )
    if obj:
        return obj

    # Try to fetch the object data from the database.
    cursor = db.connection.cursor()
    cursor.execute( get_sql, ( unique_column_val, ) )
    row = cursor.fetchone()

    # Use the provided callback to create a new instance of the object.
    # This will raise an error if the new object is not valid (i.e., if the closure
    # attempts to create the object using an invalid value).
    # It's important to do this before inserting into the database.
    obj = new_obj_callback()

    if row is not None:
        # If there was a corresponding row in the database, set the object's db_id field.
        # Since the object must have only two fields (one for lookup and db_id), this is
        # sufficient.
        obj.db_id = row[ 0 ]
    else:
        # If there was no corresponding row in the database, insert one.
        try:
            cursor.execute( insert_sql, ( unique_column_val, ) )
            obj.db_id = cursor.lastrowid

        except mariadb.Error as e:
            db.connection.rollback()
            cursor.close()
            raise e

        db.connection.commit()

    cursor.close()

    # Add the object to the cache.
    db.object_cache.set_obj( cache_key, obj )

    return obj