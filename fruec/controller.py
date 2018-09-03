import logging

from fruec.log_file import LogFileStatus
from fruec.event_type import EventType
from fruec.stats import StatCollection
from fruec import log_file_manager, db, event

_CONSUMIBLE_LP_PROJECT_IDENTIFIERS = [ 'donatewiki' ]

_logger = logging.getLogger( __name__ )


def consume_events(
    event_type,
    db_settings,
    timestamp_format_in_filenames,
    extract_timestamp_regex,
    extract_sample_rate_regex,
    directory,
    file_glob,
    default_str_validation_regex,
    detail_languages,
    detail_projects_regex,
    lp_max_batch,
    from_latest = False,
    from_time = None,
    to_time = None
):

    if from_latest and from_time:
        raise ValueError( 'Can\'t set both from_latest and from_time.' )

    # Open db connection
    db.connect( **db_settings )

    # Check no files are in partially processed state
    if db.log_file_mapper.files_with_processing_status( event_type ):
        raise RuntimeError(
            'Files with processing status found. A previous execution was probably '
            'interrupted before completion. Back up the database and purge '
            'data from incomplete processing.'
        )

    # For from_latest option, get the most recent time of all consumed files
    if from_latest:
        from_time = db.log_file_mapper.get_lastest_time( event_type )
        if from_time is None:
            _logger.warn(
                'Requested processing files from latest time previously consumed, '
                'but no latest time was found. Processing with no \'from\' limit' )

    # Get a list of objects with info about files to try
    file_infos = log_file_manager.file_infos(
        timestamp_format = timestamp_format_in_filenames,
        extract_timetamp_regex = extract_timestamp_regex,
        directory = directory,
        file_glob = file_glob,
        from_time = from_time,
        to_time = to_time
    )

    stats = StatCollection()
    stats.new_stat( 'consumed_files', 'Files consumed' )
    stats.new_stat( 'skipped_files',
        'Files selected by options, but skipped due to previous processing' )
    stats.new_stat( 'consumed_events', 'Events consumed' )
    stats.new_stat( 'ignored_events', 'Events ignored' )
    stats.new_stat( 'invalid_events', 'Invalid events' )

    for file_info in file_infos:
        filename = file_info[ 'filename' ]
        directory = file_info[ 'directory' ]

        # Skip any files already known to the db
        if db.log_file_mapper.known( filename ):
            _logger.debug( 'Skipping already processed {}.'.format( filename ) )
            stats.increment( 'skipped_files', 1 )
            continue

        _logger.debug( 'Processing {}.'.format( filename ) )

        # Create a new file object and insert it in the database
        file = db.log_file_mapper.new(
            filename = filename,
            directory = directory,
            time = file_info[ 'time' ],
            event_type = event_type,
            status = LogFileStatus.PROCESSING
        )

        # Process the file according to the indicated event type
        # This will also set per-file statistics on the file object sent
        if event_type == EventType.CENTRAL_NOTICE:
            file.sample_rate = log_file_manager.sample_rate(
                file.filename,
                extract_sample_rate_regex
            )
            _process_cn_file( file, detail_languages, detail_projects_regex,
                default_str_validation_regex )

        elif event_type == EventType.LANDING_PAGE:
            _process_lp_file( file, lp_max_batch, default_str_validation_regex )

        else:
            raise ValueError( 'Unknown event type: {}'.format( event_type ) )

        # Mark the file as consumed and update it in the DB
        file.status = LogFileStatus.CONSUMED
        db.log_file_mapper.save( file )

        stats.increment( 'consumed_files', 1 )
        stats.increment( 'consumed_events', file.consumed_events )
        stats.increment( 'ignored_events',  file.ignored_events )
        stats.increment( 'invalid_events', file.invalid_events )

    db.close()
    return stats


def purge_incomplete( event_type, db_settings ):
    _logger.debug( 'Purging data and file records for files with processing status.' )
    db.connect( **db_settings )
    stats = StatCollection()

    if event_type == EventType.CENTRAL_NOTICE:
        cells_deleted = db.cn_event_aggregator.delete_with_processing_status()
        stats.new_stat( 'deleted', 'bannerimpressions rows deleted', cells_deleted )

    elif event_type == EventType.LANDING_PAGE:
        ( lp_raw_del, dwu_del ) = db.lp_event_writer.delete_with_processing_status()

        stats.new_stat( 'lp_raw_del', 'landingpageimpression_raw rows deleted',
            lp_raw_del )

        stats.new_stat( 'dwu_del', 'donatewiki_unique rows deleted', dwu_del )

    else:
        raise ValueError( 'Unknown event type: {}'.format( event_type ) )

    files_del = db.log_file_mapper.delete_with_processing_status( event_type )
    stats.new_stat( 'files_del', 'files rows deleted', files_del )

    db.close()
    return stats


def _process_cn_file( file, detail_languages, detail_projects_regex,
        default_str_validation_regex ):

        # Count events consumed, events ignored and invalid lines for this file
        file.consumed_events = 0
        file.ignored_events = 0
        file.invalid_events = 0

        # Start aggregation step (to aggregate data per-file)
        aggregation_step = db.cn_event_aggregator.new_cn_aggregation_step(
            file,
            detail_languages,
            detail_projects_regex
        )

        # Cycle through the lines in the file, create and aggregate the events
        for line, line_no in log_file_manager.lines( file ):
            event = db.cn_event_aggregator.new_unsaved( line, default_str_validation_regex )

            # Events arrive via a public URL. Some validation happens before they
            # get here, but we do a bit more.
            if not event.valid:
                file.invalid_events += 1
                _logger.debug( 'Invalid data on line {} of {}: {}'.format(
                    line_no, file.filename, line ) )

                continue

            # Ignore events from declared bots, previews or banner not shown
            if event.bot or event.testing or ( not event.banner_shown ):
                file.ignored_events += 1
                continue

            aggregation_step.add_event( event )
            file.consumed_events += 1

        # Finish the aggregation (inserts aggregated data from the file in the db)
        aggregation_step.save()


def _process_lp_file( file, lp_max_batch, default_str_validation_regex ):

    # Count events consumed, events ignored and invalid lines for this file
    file.consumed_events = 0
    file.ignored_events = 0
    file.invalid_events = 0

    write_step = db.lp_event_writer.new_lp_write_step( file, lp_max_batch )

    # Cycle through the lines in the file, create and save the events
    for line, line_no in log_file_manager.lines( file ):
        event = db.lp_event_writer.new_unsaved( line, default_str_validation_regex )

        # Events arrive via a public URL. Some validation happens before they
        # get here, but we do a bit more.
        if not event.valid:
            file.invalid_events += 1
            _logger.debug( 'Invalid data on line {} of {}: {}'.format(
                line_no, file.filename, line ) )

            continue

        # Ignore events from declared bots
        if ( event.bot or
            ( event.project_identifier not in _CONSUMIBLE_LP_PROJECT_IDENTIFIERS ) ):
            file.ignored_events += 1
            continue

        write_step.add_event_and_maybe_write( event )
        file.consumed_events += 1

    write_step.write_events_not_yet_written()