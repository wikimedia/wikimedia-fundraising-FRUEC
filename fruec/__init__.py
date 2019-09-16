import logging

DEFAULT_CONFIG_FILENAMES = [ './fruec.yaml', '/etc/fruec.yaml' ]

LOG_FORMAT = '{levelname} {msg} ({filename} line {lineno})'


def setup_logging( debug ):
    logging.basicConfig( format = LOG_FORMAT, style = '{' )

    if debug:
        logging.root.setLevel( level = logging.DEBUG ) 
    else:
        logging.root.setLevel( level = logging.WARNING )
