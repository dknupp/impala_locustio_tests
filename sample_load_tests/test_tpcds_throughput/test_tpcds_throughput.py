"""
A simple/sample TPC-DS load test.

Randomly selects from a directory of SQL queries, and runs them concurrently
using the specified number of workers.
"""
import locust
import logging
import os
import pprint
import time

from gevent.lock import Semaphore
from gevent.exceptions import LoopExit
from impala_loadtest import DbApiLocust, TestConfig, test_setup
from impala_loadtest.common import Workloads, parse_sql_file
from locust.exception import StopLocust

logging.basicConfig()
LOG = logging.getLogger('test_tcpds_throughput')

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG_FILE = os.path.join(CURRENT_DIR, 'test_params.yaml')
DEFAULT_NUM_ITERATIONS = 5
QUERIES_DIR = Workloads.get_queries_directory('TPCDS')

WARMUP_LOCK = Semaphore()
WARMUP_FLAG = False

LOCUST_COUNTER_LOCK = Semaphore()
LOCUST_BUSY_COUNTER = 0

# Parse the config file using the event handler defined in common.py
# Config file path can be overridden with a CONFIG environment variable.
test_setup.fire(config_file=os.getenv('CONFIG', DEFAULT_CONFIG_FILE))


def increment_locust_busy_counter():
  global LOCUST_BUSY_COUNTER  # make counter accessible inside the with- block
  with LOCUST_COUNTER_LOCK:
    LOCUST_BUSY_COUNTER += 1
  LOG.debug("LOCUST_BUSY_COUNTER: {}".format(LOCUST_BUSY_COUNTER))


def decrement_locust_busy_counter():
  global LOCUST_BUSY_COUNTER  # make counter accessible inside the with- block
  with LOCUST_COUNTER_LOCK:
    LOCUST_BUSY_COUNTER -= 1
  LOG.debug("LOCUST_BUSY_COUNTER: {}".format(LOCUST_BUSY_COUNTER))


def run_warmup(client, query_files):
  global WARMUP_FLAG
  WARMUP_FLAG = True

  for query_file in query_files:
    query_str = parse_sql_file(os.path.join(QUERIES_DIR, query_file))
    LOG.info("Locust {0} warming cache for {1}".format(id(client), query_file))
    client.query(query_str=query_str)

  LOG.info("Warmup complete")


class TpcdsThroughput(locust.TaskSequence):
  """Workload for measuring throughput of select TPC queries."""
  queries = sorted(os.listdir(QUERIES_DIR))

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

    if TestConfig['client_type'] == 'OdbcClient':
      client_kwargs['socket_timeout'] = 0

    LOG.debug(pprint.pformat(client_kwargs))

    self.client.hatch(**client_kwargs)  # Instantiate the underlying client
    self.client.query('use {target_db}'.format(target_db=TestConfig['target_db']))
    LOG.info("Locust {} hatched".format(id(self.client)))
    increment_locust_busy_counter()
    time.sleep(1)

  @locust.seq_task(1)
  def warmup(self):
    """
    If warmup is true, run each query once.

    No stats will be generated.

    LOCUST_BUSY_COUNTER is decremented to indicate that the locust is ready.
    """
    global WARMUP_FLAG

    if TestConfig.get('warmup') is True:
      with WARMUP_LOCK:
        # We only need to warm up the queries with the first locust client, so we
        # set the WARMUP_FLAG to True, and block any subsequent locusts. When the
        # the lock is released, the remaining locusts will check the flag, then
        # move on to the run_queries task.
        if WARMUP_FLAG is False:
          WARMUP_FLAG = True
          for query_file in self.queries:
            query_str = parse_sql_file(os.path.join(QUERIES_DIR, query_file))
            LOG.info("Warming cache for {}".format(query_file))
            self.client.query(query_str=query_str)
          LOG.info("Warmup complete")

    LOG.info("Locust {} is ready".format(id(self.client)))
    decrement_locust_busy_counter()

  @locust.seq_task(2)
  def run_queries(self):
    """
    Sequentially iterate through available queries
    """
    while LOCUST_BUSY_COUNTER > 0:
      LOG.info("Waiting for all locusts to become ready. "
               "LOCUST_BUSY_COUNTER = {}".format(LOCUST_BUSY_COUNTER))
      time.sleep(1)

    LOG.info("Locust {}: starting test".format(id(self.client)))

    for query_file in self.queries:
      query_str = parse_sql_file(os.path.join(QUERIES_DIR, query_file))

      for _ in range(TestConfig.get('num_iterations')):
        self.client.logged_query(query_str=query_str, query_name=query_file)

    LOG.info("Locust {}: all queries completed".format(id(self.client)))

  @locust.seq_task(3)
  def stop_locust(self):
    """
    Stop the locust after all queries have been run.

    When all the locusts have stopped, the test runner will quit.
    """
    LOG.info("Stopping: locust {}".format(id(self.client)))
    raise StopLocust()


class ImpalaUser(DbApiLocust):
  """A worker capable of executing the given task set."""
  task_set = TpcdsThroughput
  wait_time = locust.between(TestConfig['min_wait'], TestConfig['max_wait'])
