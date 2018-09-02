from setuptools import setup

setup(
    name = 'fruec',
    version  = '0.1',
    description = 'Command-line utilities to process and store user-facing events for WMF Fundraising',
    license = 'GPL',
    packages = [ 'fruec' ],
    install_requires = [
        'pyyaml >= 3.11',
        # FIXME check that this is the correct package
        'mysql-connector-python >= 1.2.3'
    ],
    scripts = [
       'bin/fruec'
   ]
)