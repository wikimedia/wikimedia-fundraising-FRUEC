class StatCollection:

    def __init__( self ):
        self._stats = {}


    def new_stat( self, key, description ):
        self._stats[ key ] = Stat( description )


    def increment( self, key, amount ):
        self._stats[ key ].val += amount


    def describe( self ):
        return [ stat.describe() for stat in self._stats.values() ]


class Stat:

    def __init__( self, description ):
        self._description = description
        self.val = 0


    def describe( self ):
        return self._description + ': ' + str( self.val )