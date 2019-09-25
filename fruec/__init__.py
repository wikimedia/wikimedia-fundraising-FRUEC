import logging

CONFIG_FILENAME = 'fruec.yaml'
LOG_FORMAT = '{levelname} {msg} ({filename} line {lineno})'


def setup_logging( debug ):
    logging.basicConfig( format = LOG_FORMAT, style = '{' )

    if debug:
        logging.root.setLevel( level = logging.DEBUG ) 
    else:
        logging.root.setLevel( level = logging.WARNING )
