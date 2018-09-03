import re
from datetime import timedelta
import logging
import mysql.connector as mariadb

from fruec.cn_event import CNEvent
from fruec.db import project_mapper, language_mapper, country_mapper
from fruec import db


# SQL template for inserting a row in bannerimpressions. (Each row is a "data cell" with
# aggregated data from one or more CentralNotice events).
_INSERT_DATA_CELL_SQL = (
    'INSERT INTO bannerimpressions ('
    '  timestamp,'
    '  banner,'
    '  campaign,'
    '  project_id,'
    '  language_id,'
    '  country_id,'
    '  count,'
    '  file_id'
    ') '
    'VALUES ('
    '  %(timestamp)s,'
    '  %(banner)s,'
    '  %(campaign)s,'
    '  %(project_id)s,'
    '  %(language_id)s,'
    '  %(country_id)s,'
    '  %(count)s,'
    '  %(file_id)s'
    ')'
)

# SQL template for purging bannerimpressions rows from files in a state of incomplete
# processing.
_DELETE_DATA_FROM_FILES_WITH_PROCESSING_STATUS_SQL = (
    'DELETE'
    '  bannerimpressions '
    'FROM'
    '  bannerimpressions '
    'INNER JOIN'
    '  files '
    'ON'
    '  bannerimpressions.file_id = files.id '
    'WHERE'
    '  files.status  = \'processing\''
)

# Strings for languages and projects not separated out, from legacy.
# Following legacy, this is different from the 'default' language for LandingPages,
# which fall back to 'en' when no language is provided.
_OTHER_LANGUAGE_CODE = 'other'
_OTHER_PROJECT_IDENTIFIER = 'other_project'

_other_project = None
_other_language = None

_logger = logging.getLogger( __name__ )


def new_unsaved( json_string, default_str_validation_regex ):
    return CNEvent( json_string, default_str_validation_regex )


def new_cn_aggregation_step( file, detail_languages, detail_projects_regex ):
    return CNAggregationStep( file, detail_languages, detail_projects_regex )


def delete_with_processing_status():
    cursor = db.connection.cursor()
    try:
        cursor.execute( _DELETE_DATA_FROM_FILES_WITH_PROCESSING_STATUS_SQL )
    except mariadb.Error as e:
        db.connection.rollback()
        cursor.close()
        raise e

    db.connection.commit()
    cursor.close()
    return cursor.rowcount


def _get_other_project():
    global _other_project
    if _other_project is None:
        _other_project = project_mapper.get_or_new( _OTHER_PROJECT_IDENTIFIER )
    return _other_project


def _get_other_language():
    global _other_language
    if _other_language is None:
        _other_language = language_mapper.get_or_new( _OTHER_LANGUAGE_CODE )
    return _other_language


def _data_cell_id( time, banner, campaign, project, language, country ):
    return (
        time.strftime( '%Y%m%d%H%M%S' ) +
        banner +
        campaign +
        project.identifier +
        language.language_code +
        country.country_code
    )


class CNAggregationStep:

    def __init__( self, file,  detail_languages, detail_projects_regex ):
        self._detail_languages = detail_languages
        self._detail_projects_pattern = re.compile( detail_projects_regex )
        self._sample_rate_multiplier = 100 / file.sample_rate
        self._file = file
        self._data = {}


    def add_event( self, event ):
        # Grouping less-common projects and languages
        if self._detail_projects_pattern.match( event.project_identifier ):
            project = project_mapper.get_or_new( event.project_identifier )
        else:
            project = _get_other_project()

        if event.language_code in self._detail_languages:
            language = language_mapper.get_or_new( event.language_code )
        else:
            language = _get_other_language()

        # Remove seconds and microseconds from time to group by minute
        time = event.time - timedelta( seconds = event.time.second,
            microseconds = event.time.microsecond )

        banner = event.banner
        campaign = event.campaign
        country = country_mapper.get_or_new( event.country_code )

        cell_id = _data_cell_id( time, banner, campaign, project, language, country )

        cell = self._data.get( cell_id )
        if not cell:
            cell = _CNDataCell( time, banner, campaign, project, language, country )
            self._data[ cell_id ] = cell

        cell.event_count += self._sample_rate_multiplier


    def save( self ):
        _logger.debug( 'Aggregating {} centralnotice data cells'.format( len( self._data ) ) )

        cursor = db.connection.cursor()

        for cell in self._data.values():

            try:
                cursor.execute( _INSERT_DATA_CELL_SQL, {
                    'timestamp': cell.time,
                    'banner': cell.banner,
                    'campaign': cell.campaign,
                    'project_id': cell.project.db_id,
                    'language_id': cell.language.db_id,
                    'country_id': cell.country.db_id,
                    'count': cell.event_count,
                    'file_id': self._file.db_id
                } )

            except mariadb.Error as e:
                db.connection.rollback()
                cursor.close()
                raise e

        db.connection.commit()
        cursor.close()


class _CNDataCell:
    def __init__( self, time, banner, campaign, project, language, country ):
        self.time = time
        self.banner = banner
        self.campaign = campaign
        self.project = project
        self.language = language
        self.country = country

        self.event_count = 0
