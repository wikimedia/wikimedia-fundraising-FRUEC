import re
import logging

from fruec import project, language
from fruec.event import Event


_STR_FIELD_LIMITS = {
    'banner': 255,
    'campaign': 255
}

validate_banner_pattern = re.compile( '^[A-Za-z0-9_]+$' ) # Coordinate with CentralNotice

_logger = logging.getLogger( __name__ )


class CNEvent( Event ):
    """A CentralNotice event."""


    def __init__( self, json_string, default_str_validation_regex ):
        """Create a CentralNotice event.

        :param str json_string: Raw JSON with event data, as extracted from the log file.
        :param str default_str_validation_regex: Regex to use to validate string fields by
            default (used for string fields that don't have more specific validation
            requirements).
        """

        # The parent constructor parses the JSON and validates and sets fields common
        # to both event types.
        super().__init__( json_string, default_str_validation_regex )

        # Bow out if data was marked as invalid by the parent class
        if self.valid == False:
            return

        self.testing = self._data[ 'event' ].get( 'testingBanner', False )
        self.banner_shown = self._data[ 'event' ][ 'statusCode' ] == '6' # Received as string

        # uselang is required by the EventLogging schema, so we can be sure it's there.
        self.language_code = self._data[ 'event' ][ 'uselang' ]
        if not language.is_valid_language_code( self.language_code ):
            _logger.debug( 'Invalid language code: {}'.format( self.language_code ) )
            self.valid = False
            return

        # db is also required by the EventLogging schema.
        self.project_identifier = self._data[ 'event' ][ 'db' ]
        if not project.is_valid_identifier( self.project_identifier ):
            self.valid = False
            return

        # banner can be absent (in which case get() defaults to None)
        self.banner = self._data[ 'event' ].get( 'banner' )
        if ( self.banner is not None ) and not validate_banner_pattern.match( self.banner ):
            _logger.debug( 'Invalid banner: {}'.format( self.banner ) )
            self.valid = False
            return

        # campaign can be absent
        self.campaign = self._data[ 'event' ].get( 'campaign' )
        if ( self.campaign is not None ) and not self._is_str_default_valid( self.campaign ):
            _logger.debug( 'Invalid campaign: {}'.format( self.campaign ) )
            self.valid = False
            return

        # Something is wrong if this isn't a banner preview (testing) but there's no
        # campaign
        if ( not self.campaign ) and ( not self.testing ):
            _logger.debug( 'No campaign found, and not a banner preview.' )
            self.valid = False
            return

        self.valid = True

        # Following legacy, events are valid and will still be consumed if one or more
        # values are too long
        self._truncate_fields( _STR_FIELD_LIMITS )
