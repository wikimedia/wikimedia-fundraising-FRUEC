import mysql.connector as mariadb
import logging

from fruec.lp_event import LPEvent
from fruec.db import project_mapper, language_mapper, country_mapper
from fruec import db

_INSERT_LP_RAW_SQL = (
    'INSERT INTO landingpageimpression_raw ('
    '  timestamp,'
    '  utm_source,'
    '  utm_campaign,'
    '  utm_medium,'
    '  utm_key,'
    '  landingpage,'
    '  project_id,'
    '  language_id,'
    '  country_id,'
    '  file_id'
    ') '
    'VALUES ('
    '  %(timestamp)s,'
    '  %(utm_source)s,'
    '  %(utm_campaign)s,'
    '  %(utm_medium)s,'
    '  %(utm_key)s,'
    '  %(landingpage)s,'
    '  %(project_id)s,'
    '  %(language_id)s,'
    '  %(country_id)s,'
    '  %(file_id)s'
    ')'
)

# Using a no-op on duplicate key update; see:
# https://stackoverflow.com/questions/548541/insert-ignore-vs-insert-on-duplicate-key-update
_INSERT_DONATEWIKI_UNIQUES_SQL = (
    'INSERT INTO donatewiki_unique ('
    '  timestamp,'
    '  utm_source,'
    '  utm_campaign,'
    '  contact_id,'
    '  link_id,'
    '  file_id'
    ') '
    'VALUES ('
    '  %(timestamp)s,'
    '  %(utm_source)s,'
    '  %(utm_campaign)s,'
    '  %(contact_id)s,'
    '  %(link_id)s,'
    '  %(file_id)s'
    ') '
    'ON DUPLICATE KEY UPDATE link_id=link_id'
)

_DELETE_LP_RAW_DATA_FROM_FILES_WITH_PROCESSING_STATUS_SQL = (
    'DELETE'
    '  landingpageimpression_raw '
    'FROM'
    '  landingpageimpression_raw '
    'INNER JOIN'
    '  files '
    'ON'
    '  landingpageimpression_raw.file_id = files.id '
    'WHERE'
    '  files.status  = \'processing\''
)

_DELETE_DW_U_DATA_FROM_FILES_WITH_PROCESSING_STATUS_SQL = (
    'DELETE'
    '  donatewiki_unique '
    'FROM'
    '  donatewiki_unique '
    'INNER JOIN'
    '  files '
    'ON'
    '  donatewiki_unique.file_id = files.id '
    'WHERE'
    '  files.status  = \'processing\''
)

_logger = logging.getLogger( __name__ )


def new_unsaved( json_string, default_str_validation_regex ):
    return LPEvent( json_string, default_str_validation_regex )


def new_lp_write_step( file, lp_max_batch ):
    return LPWriteStep( file, lp_max_batch )


def delete_with_processing_status():
    cursor = db.connection.cursor()

    try:
        cursor.execute( _DELETE_LP_RAW_DATA_FROM_FILES_WITH_PROCESSING_STATUS_SQL )
        lp_raw_del = cursor.rowcount

        cursor.execute( _DELETE_DW_U_DATA_FROM_FILES_WITH_PROCESSING_STATUS_SQL )
        dw_u_del = cursor.rowcount

    except mariadb.Error as e:
        db.connection.rollback()
        cursor.close()
        raise e

    db.connection.commit()

    cursor.close()
    return ( lp_raw_del, dw_u_del )


class LPWriteStep:

    def __init__( self, file, lp_max_batch ):
        self._file = file
        self._lp_max_batch = lp_max_batch
        self._event_insert_fields = []


    def add_event_and_maybe_write( self, event ):
        self._event_insert_fields.append( _LPEventInsertFields( event, self._file ) )

        if len( self._event_insert_fields ) > self._lp_max_batch:
            self.write_events_not_yet_written()


    def write_events_not_yet_written( self ):
        _logger.debug(
            'Writing {} landingpage events'.format( len( self._event_insert_fields ) ) )

        cursor = db.connection.cursor()

        try:
            cursor.executemany( _INSERT_LP_RAW_SQL,
                [ fields.lp_raw_fields for fields in self._event_insert_fields ] )

            cursor.executemany( _INSERT_DONATEWIKI_UNIQUES_SQL,
                [ fields.donatewiki_unique_fields for fields in self._event_insert_fields ] )

        except mariadb.Error as e:
            db.connection.rollback()
            cursor.close()
            raise e

        db.connection.commit()
        cursor.close()

        self._event_insert_fields = []


class _LPEventInsertFields:

    def __init__( self, event, file ):
        self._event = event

        project = project_mapper.get_or_new( event.project_identifier )
        language = language_mapper.get_or_new( event.language_code )
        country = country_mapper.get_or_new( event.country_code )

        self.lp_raw_fields = {
            'timestamp': event.time,
            'utm_source': event.utm_source,
            'utm_campaign': event.utm_campaign,
            'utm_medium': event.utm_medium,
            'utm_key': event.utm_key,
            'landingpage': event.landingpage,
            'project_id': project.db_id,
            'language_id': language.db_id,
            'country_id': country.db_id,
            'file_id': file.db_id
        }

        self.donatewiki_unique_fields = {
            'timestamp': event.time,
            'utm_source': event.utm_source,
            'utm_campaign': event.utm_campaign,
            'contact_id': event.contact_id,
            'link_id': event.link_id,
            'file_id': file.db_id
        }