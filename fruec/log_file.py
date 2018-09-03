from enum import Enum


_FILENAME_LIMIT = 128
_DIRECTORY_LIMIT = 256


class LogFileStatus( Enum ):
    PROCESSING = 'processing'
    CONSUMED = 'consumed'


class LogFile:
    """Object representing a log file with event data."""

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
        """Create a new LogFile object.

        :param str filename: Unique filename (without directory).
        :param str directory: Directory the file was read from.
        :param fruec.event_type.EventType event_type: The type of events in the log file.
        :param fruec.log_file.LogFileStatus status: The processing status of the log file.
        :param float sample_rate: Server-side sample rate for events in the file.
        :param int consumed_events: Number of events in the file that have been consumed.
        :param int ignored_events: Number of events in the file that have been ignored.
        :param int invalid_events: Number of events in the file found to be invalid.
        """

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

        # db_id will be set by the mapper once the instance is stored
        self.db_id = None
