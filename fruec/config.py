"""Module to retrieve configuration settings."""


import yaml

from fruec import DEFAULT_CONFIG_FILENAMES

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
    for filename_to_try in DEFAULT_CONFIG_FILENAMES:
        try:
            _actually_load( filename_to_try )
            config_file_found = True

        except FileNotFoundError:
            continue

    if not config_file_found:
        raise FileNotFoundError( 'No configuration file found.' )


def _actually_load( actual_filename ):
    global _config
    with open( actual_filename, 'r' ) as stream:
        _config = yaml.load( stream )
