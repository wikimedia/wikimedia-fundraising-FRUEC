from fruec.project import Project
from fruec import db

GET_PROJECT_SQL = 'SELECT id FROM project WHERE project = %s'
INSERT_PROJECT_SQL = 'INSERT INTO project ( project ) VALUES ( %s )'
CACHE_KEY_PREFIX = 'Project'


def get_or_new( identifier ):
    cache_key = CACHE_KEY_PREFIX + identifier

    return db.lookup_on_unique_column_helper.get_or_new(
        unique_column_val = identifier,
        cache_key = cache_key,
        get_sql = GET_PROJECT_SQL,
        insert_sql = INSERT_PROJECT_SQL,
        new_obj_callback = lambda: Project( identifier )
    )
