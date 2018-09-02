from enum import Enum

class EventType(Enum):
    # output_name coordinates with the names of event types used for the command-line
    # option, where they just seems better without dashes or underscores...
    CENTRAL_NOTICE = ( 'banner', 'centralnotice' )
    LANDING_PAGE = ( 'landingpage', 'landingpage' )


    def __init__( self, legacy_key, output_name ):
        self.legacy_key = legacy_key
        self.output_name = output_name


    def __str__( self ):
        return self.output_name
