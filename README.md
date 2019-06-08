`fr_user_events_consumer`
===========================

A Python library and script for Wikimedia Foundation Fundraising. Consumes user-facing
events from log files and loads related data into a MariaDB database.

Usage
-----

Copy `config-example.yaml` to `config.yaml` and adjust settings as appropriate.
Scripts look for a configuration file in the working directory or in
`/etc/fruec`.

Command-line script is `bin/fruec`. Run it with --help for details about command-line
options. See also inline comments in `config-example.yaml` and sample log files
in the `test_data/` directory.

Regardless of the options selected, events will only be consumed from files that have not
been marked as already processed.

Filenames
----------

Log filenames must be globally unique across all log types consumed by the scripts.
Maximum length for filenames is 128 characters.

Timestamps in filenames must be in Ymd-HMS format as output by time.strftime().

Filenames ending in '.gz' are assumed to be compressed with gzip. Otherwise, there are
read as plain text.

Database
--------

Required tables can be created by executing the SQL in `sql/create_tables.sql`.
For WMF production, a previously existing database will be used; nonetheless, all the tables
on production accessed by this script will have the columns, keys and indices specified by
that file.

For developer setup, create a database and a database user, grant the user rights on
the database, then run the following command (substituting database, user and password
as appropriate):

`mariadb -u fruec --password=pwd_for_fruec fruec < sql/create_tables.sql`

For development purposes, the SQL to drop all tables is also provided. To use it, copy
`sql/drop_tables_example.sql` as `sql/drop_tables.sql` and uncomment the last two
two lines.

Then, you can reset the database like this:

`cat sql/drop_tables.sql sql/create_tables.sql | mariadb -u fruec --password=pwd_for_fruec fruec`

(Do not deploy an uncommented version of the drop tables file to production. Using the
filename `sql/drop_tables.sql` for the uncommented version will prevent it from being
added to the Git repository.)

Installation
------------

For development, try this command (from the repository root directory):

`pip3 install -e .`
