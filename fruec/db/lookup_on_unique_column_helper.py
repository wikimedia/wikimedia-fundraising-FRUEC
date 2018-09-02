import mysql.connector as mariadb

from fruec import db

def get_or_new( unique_column_val, cache_key, get_sql, insert_sql, new_obj_callback ):

    obj = db.get_cached_object( cache_key )
    if obj:
        return obj

    cursor = db.connection.cursor()
    cursor.execute( get_sql, ( unique_column_val, ) )
    row = cursor.fetchone()

    # This will raise an error if the new object is not valid
    # It's important to do this before inserting into the database.
    obj = new_obj_callback()

    if row is not None:
        obj.db_id = row[ 0 ]
    else:
        try:
            cursor.execute( insert_sql, ( unique_column_val, ) )
            obj.db_id = cursor.lastrowid

        except mariadb.Error as e:
            db.connection.rollback()
            cursor.close()
            raise e

        db.connection.commit()

    cursor.close()
    db.set_object_in_cache( cache_key, obj )
    return obj