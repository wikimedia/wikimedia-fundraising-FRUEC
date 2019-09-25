"""Module to retrieve configuration settings."""


import yaml
import os

from fruec import CONFIG_FILENAME

directories_to_try = [ os.path.dirname( os.path.realpath( __file__ ) ) + '/../', '/etc/' ]
"""Directories to search for configuration file."""

filename = None
"""Non-default configuration file to load. (To load from default locations, leave this
set to None.)"""

_config = None


def get():
    """Return configuration object. This method loads configuration from the appropriate
    yaml file the first time it's called."""

    if _config is None:
        _load()

    return _config


def _load():
    """Load the config file. If a non-default filename is set, use that. Otherwise,
    look in default locations.
    """

    if filename is not None:
        _actually_load( filename )
        return

    config_file_found = False
    for directory_to_try in directories_to_try:
        print( directory_to_try + CONFIG_FILENAME )
        try:
            _actually_load( directory_to_try + CONFIG_FILENAME )
            config_file_found = True
            break

        except FileNotFoundError:
            continue

    if not config_file_found:
        raise FileNotFoundError( 'No configuration file found.' )


def _actually_load( actual_filename ):
    global _config
    with open( actual_filename, 'r' ) as stream:
        _config = yaml.load( stream )
