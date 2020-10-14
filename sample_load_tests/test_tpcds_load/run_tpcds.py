import argparse
import os
import sqlparse
import qe_client_lib.dbapi_clients as client_lib


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
QUERIES_DIR = os.path.join(CURRENT_DIR, 'TPCDS', 'queries')

parser = argparse.ArgumentParser()
parser.add_argument('--ref-coordinator', help='Hostname of ref Impala coordinator',
                    default='localhost')
parser.add_argument('--test-coordinator', help='Hostname of test Impala coordinator',
                    default='localhost')
parser.add_argument('--use-kerberos', action='store_true', default=False,
                    help='Use kerberos to connect')
parser.add_argument('--use-ssl', action='store_true', default=False,
                    help='Use ssl to connect')
parser.add_argument('--use-http', action='store_true', default=False,
                    help='Use http to connect')
parser.add_argument('--confirm', action='store_true', default=False,
                    help='Run result confirmation')
parser.add_argument('--compare-by-hash', action='store_true', default=False,
                    help='Run result confirmation by hashing results')
parser.add_argument('--client-type', help='Impala client type', default='ImpylaClient')
parser.add_argument('--ref-db', help='Ref DB name', default='tpcds_10_decimal_parquet')
parser.add_argument('--test-db', help='Ref DB name', default='tpcds_10_decimal_parquet')
parser.add_argument('--sql-directory', help='Root of dataload SQL files',
                    default=QUERIES_DIR)
parser.add_argument('--query-file', help='Query file to run')

args = parser.parse_args()

no_match = [
    '56.sql',
    '71.sql',
    '65.sql',
    '64.sql',
    '39.sql',   # this worked once
    '31.sql',   # this worked once
]

matched = [
    '21.sql'
    '34.sql',
    '20.sql',
    '37.sql',
    '33.sql',
    '26.sql',
    '32.sql',
    '30.sql',
    # '31.sql',
    '25.sql',
    '19.sql',
    '81.sql',
    '95.sql',
    '42.sql',
    '4.sql',
    '57.sql',
    '43.sql',
    '94.sql',
    '96.sql',
    '82.sql',
    '7.sql',
    '55.sql',
    '69.sql',
    '68.sql',
    '40.sql',
    '54.sql',
    '83.sql',
    '97.sql',
    '93.sql',
    '78.sql',
    '50.sql',
    '2.sql',
    '51.sql',
    '3.sql',
    '79.sql',
    '92.sql',
    '84.sql',
    '90.sql',
    '47.sql',
    '1.sql',
    '53.sql',
    '52.sql',
    '46.sql',
    '91.sql',
    '85.sql',
    '88.sql',
    '63.sql',
    '76.sql',
    '62.sql',
    '89.sql',
    '74.sql',
    '60.sql',
    '49.sql',
    '61.sql',
    '75.sql',
    '59.sql',
    '58.sql',
    '99.sql',
    '66.sql',
    '72.sql',
    '73.sql',
    '98.sql',
    '28.sql',
    '29.sql',
    '15.sql',
    '17.sql',
    '16.sql',
    '12.sql',
    # '39.sql',
    '11.sql',
]


def parse_sql_file(sql_file):
  """Parse .sql file, and return formatted query as a string."""
  with open(sql_file) as fh:
    # Filter out comments, which are denoted by '--' in SQL files
    sql = ' '.join([line.split('--')[0].strip() for line in fh
                    if not line.startswith('--')])
  return sqlparse.format(sql, reindent=True, keyword_case='upper')


def get_reference_results(client, query_str):
    ref_results = client.query(query_str=query_str)
    return ref_results


def get_test_results(client, query_str):
    test_results = client.query(query_str=query_str)
    return test_results


def confirm_results(ref_client, test_client):
    for query_file in os.listdir(args.sql_directory):
        query_str = parse_sql_file(os.path.join(args.sql_directory, query_file))

        try:
            ref_results = get_reference_results(ref_client, query_str)
            test_results = get_test_results(test_client, query_str)
            assert compare_results(ref_results, test_results)
            print '{}: matched'.format(query_file)
        except AssertionError:
            print '{}: DID NOT MATCH!!!'.format(query_file)


def compare_results(expected_results, actual_results):
    try:
        assert len(expected_results) == len(actual_results), \
            "Number of actual rows differs from number of expected rows"
    except AssertionError as e:
        print(e)
        return False

    for expected_row, actual_row in zip(expected_results, actual_results):
        if args.compare_by_hash:
            try:
                assert hash(expected_row) == hash(tuple(actual_row)), \
                    "{} != {}".format(expected_row, actual_row)
            except AssertionError as e:
                print(e)
                return False
        else:
            for expected_val, actual_val in zip(expected_row, actual_row):
                try:
                    assert expected_val == actual_val, \
                        "{} != {}".format(expected_row, actual_row)
                except AssertionError as e:
                    print(e)
                    return False
    return True


def main():
    RefClientType = getattr(client_lib, 'ImpylaClient')
    ref_client = RefClientType(args.ref_coordinator)
    # ref_client = RefClientType('quasar-kqhtnc-4.vpc.cloudera.com',
    #                            auth_type='kerberos', ssl=True)
    ref_client.query('use {}_s3'.format(args.ref_db))

    TestClientType = getattr(client_lib, args.client_type)
    test_client = TestClientType(args.test_coordinator)
    test_client.query('use {}'.format(args.test_db))

    if args.confirm:
        confirm_results(ref_client, test_client)
    else:
        query_str = parse_sql_file(os.path.join(args.sql_directory, args.query_file))
        print query_str
        ref_results = get_reference_results(ref_client, query_str)
        test_results = get_test_results(test_client, query_str)
        assert compare_results(ref_results, test_results)


if __name__ == "__main__":
    main()
