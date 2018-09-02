from fruec.country import Country
from fruec import db

GET_COUNTRY_SQL = 'SELECT id FROM country WHERE iso_code = %s'
INSERT_COUNTRY_SQL = 'INSERT INTO country ( iso_code ) VALUES ( %s )'
CACHE_KEY_PREFIX = 'Country'


def get_or_new( country_code ):
    cache_key = CACHE_KEY_PREFIX + country_code

    return db.lookup_on_unique_column_helper.get_or_new(
        unique_column_val = country_code,
        cache_key = cache_key,
        get_sql = GET_COUNTRY_SQL,
        insert_sql = INSERT_COUNTRY_SQL,
        new_obj_callback = lambda: Country( country_code )
    )
