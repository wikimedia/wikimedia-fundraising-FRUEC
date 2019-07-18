import json
import re
import logging
from datetime import datetime

from fruec import country

EVENT_TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%SZ' # Coordinate with EventLogging

_logger = logging.getLogger( __name__ )


class Event:
    """Superclass for CNEvent and LPEvent objects."""

    def __init__( self, json_string, default_str_validation_regex ):
        """Superclass constructor, called from CNEvent and LPEvent constructors. Parses
        JSON with event data and handles fields that are common for both event types.

        :param str json_string: Raw JSON with event data, as extracted from the log file.
        :param str default_str_validation_regex: Regex to use to validate string fields by
            default (used for string fields that don't have more specific validation
            requirements).
        """

        # Default string validation is used for string fields that don't have more
        # specific validation requirements.
        self._default_str_validation_pattern = re.compile( default_str_validation_regex )
        self._raw_json = json_string

        self.valid = None

        # Parse JSON and set and validate fields used in both types of event
        try:
            self._data = json.loads( json_string )
        except ValueError as e:
            _logger.debug( 'Invalid Json: {}'.format( e ) )
            self.valid = False
            return

        self._event_uuid = self._data[ 'uuid' ]

        self.country_code = self._data[ 'event' ].get( 'country' )
        if self.country_code and not country.is_valid_country_code( self.country_code ):
            _logger.debug( 'Invalid country code: {}'.format( self.country_code ) )
            self.valid = False
            return

        try:
            self.time = datetime.strptime( self._data[ 'dt' ], EVENT_TIMESTAMP_FORMAT )
        except ValueError as e:
            _logger.debug( 'Invalid timestamp: {}'.format( e ) )
            self.valid = False
            return

        self.bot = self._data[ 'userAgent' ][ 'is_bot' ]


    def _is_str_default_valid( self, s ):
        """Does this string conform to default string validation requirements?

        :param str s: The string to validate.
        :returns bool
        """

        return bool( self._default_str_validation_pattern.match( s ) )


    def _truncate_fields( self, field_names_and_limits ):
        """Silently truncate the fields of this event as indicated. (We only log a
        warning when an field is truncated, but the event remains valid.)

        :param dict field_names_and_limits: A dictionary where the keys are field names
            (implemented in the event objects as attributes) and values are the limits
            they should be truncated to, as necessary.
        """

        for field_name, limit in field_names_and_limits.items():
            val = getattr( self, field_name )
            if ( val is not None ) and len( val ) > limit:
                _logger.warn( 'Field {} too long in event {}'.format(
                    field_name, self._event_uuid ) )
                setattr( self, field_name, val[ :limit ] )
