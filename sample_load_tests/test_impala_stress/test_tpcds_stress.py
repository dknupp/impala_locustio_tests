"""
A simple/sample TPC* stress test.

Randomly selects from a directory of SQL queries, and runs them concurrently
using the specified number of workers.
"""
import csv
import locust
import logging
import os
import random
import sys
import time

from impala_loadtest import DbApiLocust, TestConfig, test_setup
from impala_loadtest.common import Workloads, parse_sql_file

logging.basicConfig()
LOG = logging.getLogger('test_impala_stress')

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG_FILE = os.path.join(CURRENT_DIR, 'dc_impala_stress_test.yaml')

# Parse the config file using the event handler defined in common.py
# Config file path can be overridden with a CONFIG environment variable.
test_setup.fire(config_file=os.getenv('CONFIG', DEFAULT_CONFIG_FILE))

if TestConfig['client_type'] != "ImpylaClient":
  LOG.error("Stress test requires the Impyla client.")
  sys.exit(1)

if TestConfig['task_weights']['cancel_query'] and '--csv' not in sys.argv:
  LOG.error("If task weight cancel_query is > 0, --csv must be specified.")
  sys.exit(1)

if TestConfig['task_weights']['run_basic_query'] < 1:
  LOG.error("Task weight run_basic_query must be >= 1.")
  sys.exit(1)


QUERIES_DIR = Workloads.get_queries_directory(TestConfig['workload'])


class ImpalaStress(locust.TaskSet):
  """Workload for running randomly-selected queries from files."""

  queries = os.listdir(QUERIES_DIR)

  def on_start(self):
    """
    The on_start handler is called once, when a Locust worker first
    hatches and before any tasks are scheduled.
    """
    client_kwargs = {
      'host': TestConfig.get('coordinator', self.locust.host),
      'client_type': TestConfig['client_type'],
      'auth_type': TestConfig['auth_type'],
      'ssl': TestConfig['ssl'],
      'thrift_transport': TestConfig['thrift_transport'],
      'user': TestConfig['user'],
      'password': TestConfig['password']
    }
    self.client.hatch(**client_kwargs)  # Instantiate the underlying client
    self.client.query('use {target_db}'.format(target_db=TestConfig['target_db']))
    self.client_id = id(self.client)

  @locust.task(TestConfig['task_weights']['run_basic_query'])
  def run_basic_query(self):
    """
    Select a query at random from the specified workload, and run it.
    """
    query_file = random.choice(self.queries)
    query_str = parse_sql_file(os.path.join(QUERIES_DIR, query_file))
    self.client.logged_query(query_str=query_str,
                             query_name=query_file)
    LOG.info("Locust {i} ran query: {q}".format(i=self.client_id, q=query_file))

  @locust.task(TestConfig['task_weights']['cancel_query'])
  def cancel_query(self):
    """
    Select a previously run query, run it asynchronously, then cancel it.

    We pick the query from the previously run queries in the requests CSV file
    so that we can use the median execution time as a basis for how long to
    wait before cancelling.
    """
    row = random.choice(self._get_requests_csv_rows())

    if row['Method'] != 'query' or 'cancelled' in row['Name']:
      # We need a valid row from a prior successful query, otherwise do nothing.
      return
    else:
      LOG.info("Locust {i} starting query: {q}".format(i=self.client_id, q=row['Name']))
      query_str = parse_sql_file(os.path.join(QUERIES_DIR, row['Name']))

    # The timeout is derived from the median time to execute.
    # Median time is logged in milliseconds.
    median_execution_time = float(row['Median response time']) / 1000
    lower_bound = median_execution_time * TestConfig['query_timeout']['lower_bound']
    upper_bound = median_execution_time * TestConfig['query_timeout']['upper_bound']
    timeout = random.uniform(lower_bound, upper_bound)

    start_time = time.time()

    try:
      self.client._cursor.execute_async(query_str)
      time.sleep(timeout)
      self.client._cursor.cancel_operation()
      total_time = int((time.time() - start_time) * 1000)
      LOG.info("Locust {i} cancelled {q} after {t} seconds".format(
        i=self.client_id, q=row['Name'], t=timeout))

      locust.events.request_success.fire(
        request_type="query", name='{} (cancelled)'.format(row['Name']),
        response_time=total_time, response_length=0)
    except Exception as e:
      total_time = int((time.time() - start_time) * 1000)
      locust.events.request_failure.fire(
        request_type="query", name='{} (cancelled)'.format(row['Name']),
        response_time=total_time, response_length=len(str(e)),
        exception=e)
      raise

  def _get_requests_csv_rows(self):
    """
    Helper function to parse the requests CSV file, and return its rows as dicts.
    """
    command_line_args = iter(sys.argv)
    while True:
      arg = next(command_line_args)
      if arg == "--csv":
        csv_filename = "{}_requests.csv".format(next(command_line_args))
        break

    with open(os.path.join(CURRENT_DIR, csv_filename), mode='r') as infile:
      return [row for row in csv.DictReader(infile)]


class ImpalaUser(DbApiLocust):
  """A worker capable of executing the given task set."""
  task_set = ImpalaStress
  wait_time = locust.between(TestConfig['min_wait'], TestConfig['max_wait'])
