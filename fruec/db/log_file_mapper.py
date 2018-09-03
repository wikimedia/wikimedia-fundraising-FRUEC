import mysql.connector as mariadb

from fruec.log_file import LogFile
from fruec import db


_FILE_KNOWN_SQL = 'SELECT EXISTS (SELECT 1 FROM files WHERE filename = %s)'

_INSERT_FILE_SQL = (
    'INSERT INTO files ('
    '  filename,'
    '  impressiontype,'
    '  timestamp,'
    '  directory,'
    '  sample_rate,'
    '  status,'
    '  consumed_events,'
    '  ignored_events,'
    '  invalid_events'
    ') '
    'VALUES ('
    '  %(filename)s,'
    '  %(impressiontype)s,'
    '  %(timestamp)s,'
    '  %(directory)s,'
    '  %(sample_rate)s,'
    '  %(status)s,'
    '  %(consumed_events)s,'
    '  %(ignored_events)s,'
    '  %(invalid_events)s'
    ')'
)

_UPDATE_FILE_SQL = (
    'UPDATE files SET'
    '  filename = %(filename)s,'
    '  impressiontype = %(impressiontype)s,'
    '  timestamp = %(timestamp)s,'
    '  directory = %(directory)s,'
    '  sample_rate = %(sample_rate)s,'
    '  status = %(status)s,'
    '  consumed_events = %(consumed_events)s,'
    '  ignored_events = %(ignored_events)s,'
    '  invalid_events = %(invalid_events)s '
    'WHERE'
    '  id = %(db_id)s'
)

_LATEST_TIME_SQL = (
    'SELECT timestamp '
    'FROM files '
    'WHERE '
    '  status = \'consumed\' '
    '  AND impressiontype = %s '
    'ORDER BY timestamp DESC '
    'LIMIT 1'
)

_FILES_WITH_PROCESSING_STATUS_SQL = (
    'SELECT EXISTS ('
    '  SELECT 1 FROM files WHERE status = \'processing\' AND impressiontype = %s LIMIT 1'
    ')'
)

_DELETE_WITH_PROCESSING_STATUS_SQL = (
    'DELETE FROM files WHERE status = \'processing\' AND  impressiontype = %s' )

_CACHE_KEY_PREFIX = 'LogFile'


def known( filename ):
    """Note: We don't include event type in the query; filenames must be unique across all
    event types."""
    if db.object_in_cache( _make_cache_key( filename ) ):
        return True

    cursor = db.connection.cursor()
    cursor.execute( _FILE_KNOWN_SQL, ( filename, ) )
    result = bool( cursor.fetchone()[ 0 ] )
    cursor.close()
    return result


def new(
        filename,
        directory,
        time,
        event_type,
        status = None,
        sample_rate = None,
        consumed_events = None,
        ignored_events = None,
        invalid_events = None
    ):

    file = LogFile( filename, directory, time, event_type, sample_rate,
        status, consumed_events, ignored_events, invalid_events )

    cursor = db.connection.cursor()

    try:
        cursor.execute( _INSERT_FILE_SQL, {
            'filename': filename,
            'impressiontype': event_type.legacy_key,
            'timestamp': time,
            'directory': directory,
            'sample_rate': sample_rate,
            'status': status.value,
            'consumed_events': consumed_events,
            'ignored_events': ignored_events,
            'invalid_events': invalid_events
        } )

        file.db_id = cursor.lastrowid

    except mariadb.Error as e:
        db.connection.rollback()
        cursor.close()
        raise e

    db.connection.commit()
    cursor.close()

    db.set_object_in_cache( _make_cache_key( filename ), file )
    return file


def save( file ):

    # Sanity check: file should already be in the cache
    if db.get_cached_object( _make_cache_key( file.filename ) ) != file:
        raise RuntimeError(
            ( 'Attempting to save existing log file {} but it\'s not in the cache, or '
            'a different object is in the cache.' ).format( file.filename )
        )

    cursor = db.connection.cursor()

    try:
        cursor.execute( _UPDATE_FILE_SQL, {
            'filename': file.filename,
            'impressiontype': file.event_type.legacy_key,
            'timestamp': file.time,
            'directory': file.directory,
            'sample_rate': file.sample_rate,
            'status': file.status.value,
            'consumed_events': file.consumed_events,
            'ignored_events': file.ignored_events,
            'invalid_events': file.invalid_events,
            'db_id': file.db_id
        } )

    except mariadb.Error as e:
        db.connection.rollback()
        cursor.close()
        raise e

    db.connection.commit()
    cursor.close()


def get_lastest_time( event_type ):
    """Get the most recent timestamp of all consumed files for a given EventType."""
    cursor = db.connection.cursor()
    cursor.execute( _LATEST_TIME_SQL, ( event_type.legacy_key, ) )
    row = cursor.fetchone()
    cursor.close()
    return row[0] if row else None


def files_with_processing_status( event_type ):
    cursor = db.connection.cursor()
    cursor.execute( _FILES_WITH_PROCESSING_STATUS_SQL, ( event_type.legacy_key, ) )
    result = bool( cursor.fetchone()[ 0 ] )
    cursor.close()
    return result


def delete_with_processing_status( event_type ):
    cursor = db.connection.cursor()
    try:
        cursor.execute( _DELETE_WITH_PROCESSING_STATUS_SQL, ( event_type.legacy_key, ) )
    except mariadb.Error as e:
        db.connection.rollback()
        cursor.close()
        raise e

    db.connection.commit()
    cursor.close()
    return cursor.rowcount


def _make_cache_key( filename ):
    return _CACHE_KEY_PREFIX + filename
