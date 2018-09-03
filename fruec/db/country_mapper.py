from fruec.country import Country
from fruec import db

_GET_COUNTRY_SQL = 'SELECT id FROM country WHERE iso_code = %s'
_INSERT_COUNTRY_SQL = 'INSERT INTO country ( iso_code ) VALUES ( %s )'
_CACHE_KEY_PREFIX = 'Country'


def get_or_new( country_code ):
    """Get a Country object for the country_code provided. If no row exists in the
    database for this code, insert one.

    :param str country_code: Code for the country, as received from the event.
    """

    cache_key = _CACHE_KEY_PREFIX + country_code

    return db.lookup_on_unique_column_helper.get_or_new(
        unique_column_val = country_code,
        cache_key = cache_key,
        get_sql = _GET_COUNTRY_SQL,
        insert_sql = _INSERT_COUNTRY_SQL,
        new_obj_callback = lambda: Country( country_code )
    )
