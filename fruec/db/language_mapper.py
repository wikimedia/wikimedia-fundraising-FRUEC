from fruec.language import Language
from fruec import db

GET_LANGUAGE_SQL = 'SELECT id FROM language WHERE iso_code = %s'
INSERT_LANGUAGE_SQL = 'INSERT INTO language ( iso_code ) VALUES ( %s )'
CACHE_KEY_PREFIX = 'Language'


def get_or_new( identifier ):
    cache_key = CACHE_KEY_PREFIX + identifier

    return db.lookup_on_unique_column_helper.get_or_new(
        unique_column_val = identifier,
        cache_key = cache_key,
        get_sql = GET_LANGUAGE_SQL,
        insert_sql = INSERT_LANGUAGE_SQL,
        new_obj_callback = lambda: Language( identifier )
    )
