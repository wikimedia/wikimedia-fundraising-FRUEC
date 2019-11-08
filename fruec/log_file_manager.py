"""A module for processing log files. Interaction with the filesystem (including
searching directories and reading files), as well as interpreting data contained in
filenames, are the exclusive responsibility of this module."""


import os
import glob
import gzip
import re
import datetime


_gzip_filename_pattern = re.compile( '.*\.gz$' )


def file_infos( timestamp_format, extract_timestamp_regex, directory, file_glob,
    from_time = None, to_time = None ):
    """Find files as specified, and return a dictionary with informaiton about them.

    :param str timestamp_format: Format of timestamps in filenames (as used by
        datatime.strptime()).
    :param str extract_timestamp_regex: Regex to extract timestamps from filenames.
    :param str directory: The root directory to look for files in (subdirectories will
        also be included).
    :param str file_glob: A filesystem glob to select log files.
    :param datetime.datetime from_time: Select files from this time onward (inclusive).
    :param datetime.datetime to_time: Select files up to this time (inclusive).
    :returns dict: A dictionary with the following keys: filename, directory and time.
    """

    if not os.path.isdir( directory ):
        raise ValueError( 'Not a directory: {}'.format( directory ) )

    if os.path.dirname( file_glob ):
        raise ValueError( 'file_glob can\'t include directory: {}'.format( file_glob ) )

    # Find subdirectories but don't follow symlinks, in case infinite recursion
    directories = [ x[0] for x in os.walk( directory, followlinks = False ) ]

    # Regex pattern for extracting timestamps from filenames
    extract_ts_pattern = re.compile( extract_timestamp_regex )

    # Check for duplicate filenames (since we're looking in subdirectories, too)
    filenames = []
    file_infos = []
    for directory in directories:
        filenames_in_dir = glob.glob( os.path.join( directory, file_glob ) )

        for fn in filenames_in_dir:
            base_fn = os.path.basename( fn )

            # Extract and parse timestamp from filename. This  will raise an error if the
            # timestamp in the filename is in the wrong format
            fn_ts_match = extract_ts_pattern.search( base_fn )
            if fn_ts_match is None:
                raise ValueError(
                    'No timestamp found in filename {} in {}'.format( base_fn, directory )
                )

            fn_ts = fn_ts_match.group( 0 )
            fn_time = datetime.datetime.strptime( fn_ts, timestamp_format )

            # Duplicate filenames not allowed, regardless of directory
            if base_fn in filenames:
                raise ValueError(
                    'Duplicate filename found: {} in {}'.format( base_fn, directory ) )

            if ( from_time is not None ) and ( fn_time < from_time ):
                continue

            if ( to_time is not None ) and ( fn_time > to_time ):
                continue

            file_infos.append( {
                'filename': base_fn,
                'directory': directory,
                'time': fn_time
            } )

    # Return file infos in chronological (and not filesystem) order
    file_infos.sort( key = lambda f: f[ 'time' ] )
    return file_infos


def sample_rate( filename, extract_sample_rate_regex ):
    """Extract sample rate data from a filename.

    :param str filename: The filename with the sample rate data.
    :param str extract_sample_rate_regex: Regex to extract sampple rate from filenames.
    :returns int
    """

    m = re.search( extract_sample_rate_regex, filename )

    if not m:
        raise RuntimeError( 'Couldn\'t extract sample rate from filename: {}'.
            format( filename ) )

    sr = int( m.group( 0 ) )

    if ( sr <= 0 ) or ( sr > 100 ):
        raise ValueError( 'Invalid sample rate {} for {}.'.format( sr, filename ) )

    return sr


def lines( file ):
    """Get a generator to iterate over lines in a file.

    :param fruec.log_file.LogFile file: The file object for the file to open and read.
    :returns generator: A generator providing tuples with line contents and line numbers.
    """

    filename = os.path.join( file.directory, file.filename )
    line_no = 1 # First line is 1

    # Open normally or with gzip, depending on the filename
    if _gzip_filename_pattern.match( file.filename ):
        with gzip.open( filename ) as stream:
            for l in stream:
                yield ( l.decode( 'utf-8' ), line_no )
                line_no += 1

    else:
        with open( filename ) as stream:
            for l in stream:
                yield ( l, line_no )
                line_no += 1
