from fruec.project import Project
from fruec import db


_GET_PROJECT_SQL = 'SELECT id FROM project WHERE project = %s'
_INSERT_PROJECT_SQL = 'INSERT INTO project ( project ) VALUES ( %s )'
_CACHE_KEY_PREFIX = 'Project'


def get_or_new( identifier ):
    """Get a Project object for the identifier provided. If no row exists in the
    database for this identifier, insert one.

    :param str identifier: Identifier for the project, as received from the event.
    """

    cache_key = _CACHE_KEY_PREFIX + identifier

    return db.lookup_on_unique_column_helper.get_or_new(
        unique_column_val = identifier,
        cache_key = cache_key,
        get_sql = _GET_PROJECT_SQL,
        insert_sql = _INSERT_PROJECT_SQL,
        new_obj_callback = lambda: Project( identifier )
    )
