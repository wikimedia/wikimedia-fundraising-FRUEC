""""
This module provides a cache for objects in the database (used by mappers).
"""


_object_cache = {}


def get_obj( key ):
    return _object_cache.get( key, None )


def set_obj( key, obj ):
    _object_cache[ key ] = obj


def is_cached( key ):
    return key in _object_cache