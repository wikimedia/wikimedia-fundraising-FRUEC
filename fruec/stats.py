class StatCollection:

    def __init__( self ):
        self._stats = {}


    def new_stat( self, key, description, val = 0 ):
        self._stats[ key ] = _Stat( description, val )


    def increment( self, key, amount ):
        self._stats[ key ].val += amount


    def describe( self ):
        return [ stat.describe() for stat in self._stats.values() ]


class _Stat:

    def __init__( self, description, val = 0 ):
        self._description = description
        self.val = val


    def describe( self ):
        return self._description + ': ' + str( self.val )