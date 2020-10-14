"""
A simple/sample TPC-DS load test.

Randomly selects from a directory of SQL queries, and runs them concurrently
using the specified number of workers.
"""
import locust
import logging
import os
import random

from impala_loadtest import DbApiLocust, TestConfig, test_setup
from impala_loadtest.common import parse_sql_file


logging.basicConfig()
LOG = logging.getLogger(__file__)
LOG.setLevel(getattr(logging, os.getenv('loglevel', 'WARNING')))

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG_FILE = os.path.join(CURRENT_DIR, 'dc_tpcds_load_test.yaml')

# Parse the config file using the event handler defined in common.py
# Config file path can be overridden with a CONFIG environment variable.
test_setup.fire(config_file=os.getenv('CONFIG', DEFAULT_CONFIG_FILE))

QUERIES_DIR = os.path.join(CURRENT_DIR, 'TPCH', 'queries')


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

  @locust.task(2)
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
