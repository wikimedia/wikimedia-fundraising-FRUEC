import re
import logging

from fr_user_event_consumer import project, language
from fr_user_event_consumer.event import Event

validate_banner_pattern = re.compile( '^[A-Za-z0-9_]+$' ) # Coordinate with CentralNotice

_logger = logging.getLogger( __name__ )

class CNEvent( Event ):

    def __init__( self, json_string, default_str_validation_regex ):
        super().__init__( json_string, default_str_validation_regex )

        # Bow out if data was marked as invalid by the parent class
        if self.valid == False:
            return

        self.testing = self._data[ 'event' ].get( 'testingBanner', False )
        self.banner_shown = self._data[ 'event' ][ 'statusCode' ] == '6' # Received as string

        # uselang is required by the EventLogging schema
        self.language_code = self._data[ 'event' ][ 'uselang' ]
        if not language.is_valid_language_code( self.language_code ):
            _logger.debug( 'Invalid language code: {}'.format( self.language_code ) )
            self.valid = False
            return

        # db is required by the EventLogging schema
        self.project_identifier = self._data[ 'event' ][ 'db' ]
        if not project.is_valid_identifier( self.project_identifier ):
            self.valid = False
            return

        # banner may be absent (in which case get() defaults to None)
        self.banner = self._data[ 'event' ].get( 'banner' )
        if ( self.banner is not None ) and not validate_banner_pattern.match( self.banner ):
            _logger.debug( 'Invalid banner: {}'.format( self.banner ) )
            self.valid = False
            return

        # campaign may be absent
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
