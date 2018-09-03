import re

validation_pattern = re.compile( '^[a-zA-Z]{2,2}$' )

def is_valid_country_code( country_code ):
    return bool( validation_pattern.match( country_code) )


class Country:

    def __init__( self, country_code ):
        """Create a country object.

        :param str country_code: Code for the country, as received from the event.
        """

        if not is_valid_country_code( country_code ):
            raise ValueError( 'Invalid country code: {}'.format( country_code ) )

        self.country_code = country_code
        self.db_id = None
