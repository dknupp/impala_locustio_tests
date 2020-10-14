"""
A simple SQL the runs randonly selected queries against the default sample data.
"""
import locust
import logging
import os
import random

from impala_loadtest import (
  DbApiLocust,
  TestConfig,
  test_setup
)

logging.basicConfig()
LOG = logging.getLogger(__file__)
LOG.setLevel(getattr(logging, os.getenv('loglevel', 'WARNING')))

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
QUERIES_DIR = os.path.join(CURRENT_DIR, 'queries')
DEFAULT_CONFIG_FILE = os.path.join(CURRENT_DIR, 'dwx_basic_load_params.yaml')

test_setup.fire(config_file=os.getenv('CONFIG', DEFAULT_CONFIG_FILE))


class RandomizedDwxQueries(locust.TaskSet):
  """
  TaskSet for running randomly-selected simple queries against the default
  DWX dataset.
  """

  queries = [
    'show databases',
    'show tables',
    'use airline_ontime_parquet',
    'use airline_ontime_orc',
    'describe airline_ontime_parquet.airports',
    'describe extended airline_ontime_parquet.airlines',
    'select version()',
    'select count(*) from airline_ontime_parquet.flights',
    'select * from airline_ontime_parquet.flights limit 25',
    'select * from airline_ontime_parquet.flights where origin = "ATL" and tailnum = "N641DL" and arrdelay > 10 limit 20',  # noqa
    'select * from airline_ontime_parquet.airports where city = "New York" or city = "San Francisco"'  # noqa
  ]

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
    self.client.hatch(**client_kwargs)
    self.client.connect()

  @locust.task
  def run_random_query(self):
    query_str = random.choice(self.queries)
    self.client.logged_query(query_str=query_str, query_name=query_str)


class ImpalaUser(DbApiLocust):
  """A worker capable of executing the given task set."""
  task_set = RandomizedDwxQueries
  wait_time = locust.between(TestConfig['min_wait'], TestConfig['max_wait'])
