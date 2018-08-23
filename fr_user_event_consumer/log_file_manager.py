import os
import glob
import gzip
import re
import datetime

_gzip_filename_pattern = re.compile( '.*\.gz$' )


def file_infos( timestamp_format, extract_timetamp_regex, directory, file_glob,
    from_time = None, to_time = None ):

    if not os.path.isdir( directory ):
        raise ValueError( 'Not a directory: {}'.format( directory ) )

    if os.path.dirname( file_glob ):
        raise ValueError( 'file_glob can\'t include directory: {}'.format( file_glob ) )

    # Find subdirectories but don't follow symlinks, in case infinite recursion
    directories = [ x[0] for x in os.walk( directory, followlinks = False ) ]

    # Regex pattern for extracting timestamps from filenames
    extract_ts_pattern = re.compile( extract_timetamp_regex )

    # Check for duplicate filenames (since we're looking in subdirectories, too)
    filenames = []
    file_infos = []
    for directory in directories:
        filenames_in_dir = glob.glob( os.path.join( directory, file_glob ) )

        for fn in filenames_in_dir:
            base_fn = os.path.basename( fn )

            # Extract and parse timestamp from filename. This  will raise an error if the
            # timestamp in the filename is in the wrong format
            fn_ts = extract_ts_pattern.search( base_fn ).group( 0 )
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
    m = re.search( extract_sample_rate_regex, filename )

    if not m:
        raise RuntimeError( 'Couldn\'t extract sample rate from filename: {}'.
            format( filename ) )

    sr = int( m.group( 0 ) )

    if ( sr <= 0 ) or ( sr > 100 ):
        raise ValueError( 'Invalid sample rate {} for {}.'.format( sr, filename ) )

    return sr


def lines( file ):
    filename = os.path.join( file.directory, file.filename )
    line_no = 1 # First line is 1

    if _gzip_filename_pattern.match( file.filename ):
        with gzip.open( filename ) as stream:
            for l in stream:
                yield ( l, line_no )
                line_no += 1

    else:
        with open( filename ) as stream:
            for l in stream:
                yield ( l, line_no )
                line_no += 1
