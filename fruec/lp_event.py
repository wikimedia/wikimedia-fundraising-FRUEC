import logging

from fruec.event import Event
from fruec import language

# Fields to extract and set using the same name in the raw event and in attributes on this
# object, and to apply default string validation to.
_STR_FIELDS_DEFAULT_VALIDATE = [
    'utm_source',
    'utm_campaign',
    'utm_medium',
    'utm_key',
    'contact_id',
    'link_id',
    'template',
    'appeal',
    'appeal_template',
    'form_template',
    'form_countryspecific',
    'landingpage'
]

# Note: Following legacy script with these defaults
_DEFAULT_COUNTRY_CODE = 'XX'
_DEFAULT_LANGUAGE_CODE = 'en'

# Setup for assembling the landingpage field, based on multiple fields in the raw event.
_LP_SPECIAL_PAGE = 'Special:LandingPage'
_LP_COMPONENT_FIELDS = [
    'template',
    'appeal_template',
    'appeal',
    'form_template',
    'form_countryspecific'
]

_LP_COMPONENT_FIELD_DEFAULT = 'default'
_LP_COMPONENT_FIELD_INTERAL_SEPARATOR = '-'
_LP_COMPONENT_FIELD_JOIN_STR = '~'

_STR_FIELD_LIMITS = {
    'utm_source': 255,
    'utm_campaign': 255,
    'utm_medium': 255,
    'utm_key': 128,
    'landingpage': 255,
    'contact_id': 255,
    'link_id': 128
}

_logger = logging.getLogger( __name__ )


class LPEvent( Event ):
    """A LandingPage event"""

    def __init__( self, json_string, default_str_validation_regex ):
        """Create a LandingPage event.

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

        self.language_code = self._data[ 'event' ].get( 'language' )

        if self.language_code is None:
            self.language_code = _DEFAULT_LANGUAGE_CODE

        elif not language.is_valid_language_code( self.language_code ):
            _logger.debug( 'Invalid language code: {}'.format( self.language_code ) )
            self.valid = False
            return

        if not ( self._set_and_default_validate_str_fields( _STR_FIELDS_DEFAULT_VALIDATE ) ):
            self.valid = False
            return

        self.project_identifier = self._data.get( 'wiki' )
        if self.project_identifier is None:
            _logger.debug( 'Missing wiki project field.' )
            self.valid = False
            return

        elif not self._is_str_default_valid( self.project_identifier ):
            _logger.debug( 'Invalid project: {}'.format( self.language_code ) )
            self.valid = False
            return

        # country_code set and validated by superclass; just set a default if missing
        self._set_missing_fields_to_default( [ 'country_code' ], _DEFAULT_COUNTRY_CODE )

        # as per legacy, these fields are set to '' when missing
        self._set_missing_fields_to_default(
            [ 'utm_source', 'utm_campaign', 'utm_medium', 'utm_key', 'contact_id', 'link_id' ],
            ''
        )

        if self.landingpage == _LP_SPECIAL_PAGE:
            # LP component fields should already have been set and validated. Here
            # we set defaults and munge them into a single field, as per legacy
            # (almost exactly).
            self._set_missing_fields_to_default( _LP_COMPONENT_FIELDS,
                _LP_COMPONENT_FIELD_DEFAULT )

            self._trim_before_field_separator( _LP_COMPONENT_FIELDS,
                _LP_COMPONENT_FIELD_INTERAL_SEPARATOR, _LP_COMPONENT_FIELD_DEFAULT )

            self.landingpage = self._join_fields( _LP_COMPONENT_FIELDS,
                _LP_COMPONENT_FIELD_JOIN_STR )

        self.valid = True

        # Following legacy, events are valid and will still be consumed if one or more
        # values are too long
        self._truncate_fields( _STR_FIELD_LIMITS )


    def _set_and_default_validate_str_fields( self, field_names ):
        for field_name in field_names:
            field_val = self._data[ 'event' ].get( field_name )

            if ( ( field_val is not None) and
                ( not self._is_str_default_valid( field_val ) ) ):
                _logger.debug( 'Invalid {}: {}'.format( field_name, field_val ) )
                return False

            setattr( self, field_name, field_val )

        return True


    def _set_missing_fields_to_default( self, field_names, default ):
        for field_name in field_names:
            if getattr( self, field_name ) is None:
                setattr( self, field_name, default )


    def _trim_before_field_separator( self, field_names, separator, default ):
        for field_name in field_names:
            val = getattr( self, field_name )
            partitioned_val = val.rpartition( separator )
            setattr( self, field_name, partitioned_val[2] or partitioned_val[0] or default )


    def _join_fields( self, field_names, join_str ):
        vals = [ getattr( self, field_name ) for field_name in field_names ]
        return join_str.join( vals )
