""""Module for database operations involving Languages."""


from fruec.language import Language
from fruec import db


# SQL templates
_GET_LANGUAGE_SQL = 'SELECT id FROM language WHERE iso_code = %s'
_INSERT_LANGUAGE_SQL = 'INSERT INTO language ( iso_code ) VALUES ( %s )'
_CACHE_KEY_PREFIX = 'Language'


def get_or_new( language_code ):
    """Get a Language object for the language_code provided. If no row exists in the
    database for this language_code, insert one.

    :param str language_code: Code for the language, as received from the event.
    """

    cache_key = _CACHE_KEY_PREFIX + language_code

    # This helper lets us re-use DB logic among country, language and project mappers
    return db.lookup_on_unique_column_helper.get_or_new(
        unique_column_val = language_code,
        cache_key = cache_key,
        get_sql = _GET_LANGUAGE_SQL,
        insert_sql = _INSERT_LANGUAGE_SQL,
        new_obj_callback = lambda: Language( language_code )
    )
