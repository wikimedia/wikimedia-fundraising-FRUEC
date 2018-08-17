#!/usr/bin/python3

"""
Compare banner and landing page impressions data across new and legacy backends.
"""

import argparse
import mysql.connector as db
import csv
from fr_user_event_consumer import config

config.filename = './tests/test_config.yaml'
DEBUG_SQL = []


def run(args):
    """
    Query both backends to pull in permutations of aggregate data and
    write results to CSV, unless --debug flag is passed, which instead
    writes output to stdout along with sql queries used.
    """
    from_datetime = None
    to_datetime = None
    mode = None
    debug = False

    # setup filter and range params
    if args.from_datetime:
        from_datetime = args.from_datetime
    if args.to_datetime:
        to_datetime = args.to_datetime
    if args.debug:
        debug = True
    if args.mode:
        mode = args.mode

    # get aggregate stats data from data sources
    overall_backend_aggregate_stats = {}
    for backend in config.get()['backends']:
        aggregated_counts_by_field_permutations = get_aggregate_stats_counts(backend, mode, from_datetime, to_datetime)
        overall_backend_aggregate_stats[backend] = aggregated_counts_by_field_permutations

    # write to output
    if debug:
        for backend in config.get()['backends']:
            print_aggregate_stats("{} stats:".format(backend.capitalize()), overall_backend_aggregate_stats[backend])
        for sql in DEBUG_SQL:
            print("\n" + sql + "\n");
    else:
        write_aggregate_stats_to_csv(overall_backend_aggregate_stats)


def print_aggregate_stats(header, results):
    print(header)
    for i, row in enumerate(results):
        result = "{}) ".format(i + 1)
        result += ", ".join(str(x) for x in row[0:-1])
        result += ", Count:{}".format(str(row[-1]))
        print(result)


def write_aggregate_stats_to_csv(overall_backend_aggregate_stats):
    directory = config.get()['output_csv_directory']
    filename = config.get()['output_csv_filename']
    headers = config.get()['output_csv_headers']
    with open(directory + filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
        writer.writerow(headers)
        for backend in config.get()['backends']:
            for row in [list(row) for row in overall_backend_aggregate_stats[backend]]:
                row.insert(0, backend.capitalize())
                writer.writerow(row)


def get_aggregate_stats_counts(backend, mode, from_datetime, to_datetime):
    db_conn = db_connect(config.get()["db_config"][backend])
    query = get_backend_aggregate_sum_query(db_conn, mode, from_datetime, to_datetime)
    result = db_fetchall(db_conn, query)
    db_conn.close()
    return result


def get_backend_aggregate_sum_query(db_conn, mode, from_datetime, to_datetime):
    db_name = getattr(db_conn, '_database')
    table_name = get_table_name_from_mode(mode)

    sql = get_select_sql(table_name)
    sql += get_from_sql(db_name, table_name)
    sql += get_join_sql()
    if from_datetime and to_datetime:
        sql += get_timestamp_between_sql(from_datetime, to_datetime)
    elif from_datetime:
        sql += get_where_sql('timestamp >= "{}"'.format(from_datetime))
    elif to_datetime:
        sql += get_where_sql('timestamp <= "{}"'.format(to_datetime))
    sql += get_group_by_sql()

    # record sql in case of debug
    DEBUG_SQL.append(sql)

    return sql


def get_select_sql(table_name):
    """
    Build up the aggregate MySQL query to be used across both backends
    """
    shared_backend_fields = config.get()['shared_backend_fields']
    shared_aggregate_sum_field = config.get()['shared_aggregate_sum_field']
    excluded_display_fields = ['project_id', 'country_id', 'language_id']

    # get rows count, start timestamp and end timestamp
    sql = """SELECT count(`{}`.`id`) AS `rows`,
min(`timestamp`) as `first`,
max(`timestamp`) as `last`,""".format(table_name)

    # get shared fields that we want to include (not excluded above)
    sql += ", ".join("`{}`".format(field) for field in shared_backend_fields if field not in excluded_display_fields)

    # get text desc of foreign key joins (e.g. project name of project_id)
    sql += ", `p`.`project`, `l`.`iso_code`, `c`.`iso_code`"

    # sum the shared_aggregate_sum_field (e.g. count)
    sql += ", SUM(`{}`) AS `total`".format(shared_aggregate_sum_field)
    return sql


def get_from_sql(db_name, table_name):
    return "FROM `{}`.`{}` ".format(db_name, table_name)


def get_join_sql():
    sql = "INNER JOIN `country` `c` ON `country_id` = `c`.`id` "
    sql += "INNER JOIN `project` `p` ON `project_id` = `p`.`id` "
    sql += "INNER JOIN `language` `l` ON `language_id` = `l`.`id` "
    return sql


def get_timestamp_between_sql(from_timestamp, to_timestamp):
    return "WHERE timestamp BETWEEN \"{}\" AND \"{}\" ".format(from_timestamp, to_timestamp)


def get_where_sql(where_clause):
    return "WHERE {} ".format(where_clause)


def get_group_by_sql():
    """
    To show all available permutations of the data, we group by all shared backend fields
    """
    shared_backend_fields = config.get()['shared_backend_fields']
    sql = "GROUP BY "
    sql += ", ".join("`{}`".format(field) for field in shared_backend_fields)
    return sql


def get_table_name_from_mode(mode):
    if mode == 'banners':
        return config.get()['db_tables']['banners']
    elif mode == 'lp':
        return config.get()['db_tables']['lp']
    else:
        return config.get()['db_tables']['banners']


def db_connect(db_config):
    connection = db.connect(**db_config)
    return connection


def db_fetchall(connection, sql):
    cursor = connection.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=
        """Generate an aggregate of backend stats data for data equivalence tests across FRUEC
        stats and Legacy stats databases. Output will be written to CSV for further 
        analysis in spreadsheet form. 
        """
    )

    parser.add_argument(
        '-m',
        '--mode',
        help="""Run comparison in either banner or lp mode.
        banner = bannerimpressions table
        lp = landingpageimpressions table
        """,
        dest='mode'
    )
    parser.add_argument(
        '-d',
        '--debug',
        help='Output results to stdout instead of CSV and also output SQL used',
        action='store_true',
        dest='debug'
    )

    parser.add_argument(
        '--from',
        help='Compare starting from a specific datetime e.g 2018-08-16 15:00:00',
        dest='from_datetime'
    )

    parser.add_argument(
        '--to',
        help='Compare up to specific a datetime e.g 2018-08-16 15:00:00',
        dest='to_datetime'
    )

    run(parser.parse_args())
