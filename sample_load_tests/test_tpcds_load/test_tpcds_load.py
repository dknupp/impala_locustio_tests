"""
A simple/sample TPC-DS load test.

Randomly selects from a directory of SQL queries, and runs them concurrently
using the specified number of workers.
"""
import locust
import logging
import os
import random
import sys
import time
import yaml

from impala_loadtest import DbApiLocust, TestConfig, test_setup
from impala_loadtest.common import DataTypeLoader, parse_sql_file


logging.basicConfig()
LOG = logging.getLogger(__file__)
LOG.setLevel(getattr(logging, os.getenv('loglevel', 'WARNING')))

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG_FILE = os.path.join(CURRENT_DIR, 'dc_tpcds_load_test.yaml')

# Parse the config file using the event handler defined in common.py
# Config file path can be overridden with a CONFIG environment variable.
test_setup.fire(config_file=os.getenv('CONFIG', DEFAULT_CONFIG_FILE))

QUERIES_DIR = os.path.join(CURRENT_DIR, 'TPCDS', 'queries')
RESULTS_DIR = os.path.join(CURRENT_DIR, 'TPCDS', TestConfig['expected_results'])


class RandomizedTpcdsQueries(locust.TaskSet):
  """Workload for running randomly-selected TPCDS queries from files."""

  queries = os.listdir(QUERIES_DIR)

  # Each time we parsed saved results from yaml to validate query
  # correctness, we cache it here for re-use to avoid having to
  # re-parse the same yaml file again and again.
  cached_results = {}

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

  @locust.task(10)
  def run_random_query(self):
    """
    Select a file at random from the directory of TPC-DS queries, and run it
    """
    query_file = random.choice(self.queries)
    query_str = parse_sql_file(os.path.join(QUERIES_DIR, query_file))
    self.client.logged_query(query_str=query_str,
                             query_name=query_file)

  @locust.task(5)
  def run_random_query_and_confirm_results(self):
    """
    Select a file at random from the directory of TPC-DS queries, run it,
    then confirm the results against a saved result set.

    Registers a locust failure event if results don't match expected values,
    otherwise, register success.
    """
    query_file = random.choice(self.queries)
    query_name = query_file.split('.')[0]  # i.e., drop .sql file extension
    query_str = parse_sql_file(os.path.join(QUERIES_DIR, query_file))

    if query_name not in self.cached_results:
      with open(os.path.join(RESULTS_DIR, '{}.yaml'.format(query_name))) as infile:
        self.cached_results[query_name] = yaml.load(infile, Loader=DataTypeLoader)

    start_time = time.time()

    # Note that in this task, we're using the proxied DBAPI client's underlying
    # query() method directly. We don't use the Locust client's logged_query()
    # because we only want to emit a single success or failure event AFTER the
    # results have been validated.
    results = self.client.query(query_str)

    try:
      assert results == self.cached_results[query_name], \
                        "Results mismatch for {}".format(query_file)  # noqa
      total_time = int((time.time() - start_time) * 1000)
      locust.events.request_success.fire(
        request_type="query", name='{} (validated)'.format(query_file),
        response_time=total_time, response_length=sys.getsizeof(results)
      )
    except AssertionError as e:
      total_time = int((time.time() - start_time) * 1000)
      locust.events.request_failure.fire(
        request_type="query", name='{} (validated)'.format(query_file),
        response_time=total_time, response_length=len(str(e)),
        exception=e
      )
      raise

  @locust.task(1)
  def reestablish_connection(self):
    """
    Disconnect and reconnect to the server.
    """
    self.client.disconnect()
    self.client.connect()
    self.client.query('use {target_db}'.format(target_db=TestConfig['target_db']))


class ImpalaUser(DbApiLocust):
  """A worker capable of executing the given task set."""
  task_set = RandomizedTpcdsQueries
  wait_time = locust.between(TestConfig['min_wait'], TestConfig['max_wait'])
