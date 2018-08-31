from enum import Enum

_FILENAME_LIMIT = 128
_DIRECTORY_LIMIT = 256


class LogFileStatus( Enum ):
    PROCESSING = 'processing'
    CONSUMED = 'consumed'


class LogFile:

    def __init__(
        self,
        filename,
        directory,
        time,
        event_type,
        sample_rate = None,
        status = None,
        consumed_events = None,
        ignored_events = None,
        invalid_events = None
    ):

        if len( filename ) > _FILENAME_LIMIT:
            raise ValueError( 'Filename too long: {}'.format( filename ) )

        if len( directory ) > _DIRECTORY_LIMIT:
            raise ValueError( 'Directory too long: {}'.format( directory ) )

        self.filename = filename
        self.directory = directory
        self.time = time
        self.event_type = event_type
        self.sample_rate = sample_rate
        self.status = status
        self.consumed_events = consumed_events
        self.ignored_events = ignored_events
        self.invalid_events = invalid_events

        self.db_id = None
