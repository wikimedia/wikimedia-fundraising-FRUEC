import re

_LANGUAGE_CODE_LIMIT = 24

validation_pattern = re.compile( '^[a-z\-_]+$' )

def is_valid_language_code( language_code ):
    return ( ( len( language_code ) <= _LANGUAGE_CODE_LIMIT ) and
        bool( validation_pattern.match( language_code) ) )


class Language:

    def __init__( self, language_code ):
        if not is_valid_language_code( language_code):
            raise ValueError( 'Invalid language code: {}'.format( language_code ) )

        self.language_code = language_code
        self.db_id = None