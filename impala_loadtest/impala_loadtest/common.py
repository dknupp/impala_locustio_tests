"""Common classes and helpers for setting up a Locaust load test of Impala."""

import impala.dbapi as impyla
import locust
import logging
import sqlparse
import time
import yaml

from cm_api.api_client import ApiResource
from contextlib import contextmanager
from impala.error import ProgrammingError

DEFAULT_HS2_PORT = 21050

logger = logging.getLogger(name='impala_loadtest')

# Reduce the chattiness of hiveserver2 logging
logging.getLogger('impala.hiveserver2').setLevel(logging.WARNING)


class TestConfig(object):
    """Test configurations that can be imported by a Locust test file."""

    config = {}
    nodes = []


class ImpylaLocustClient(object):
    """
    A wrapper around an Impyla client that fires Locust success/failure events.

    Each ImpylaLocust contains an an instance of this class, which connects to
    and execute queries on an impalad. On query success, the client registers
    a Locust request_success event, and on query failure, a request_failure
    event is registered. This allows requests to get tracked in Locust's
    statistics.
    """

    def __init__(self):
        self.impalad = None
        self.conn = None
        self.cursor = None

    @property
    def hostname(self):
        return self.impalad.split('.')[0]

    def connect(self, impalad_host, hs2_port=DEFAULT_HS2_PORT):
        """
        Connect to the supplied impalad instance.

        Called once by each Locust worker.
        """
        self.impalad = impalad_host
        self.conn = impyla.connect(impalad_host, hs2_port)
        self.cursor = self.conn.cursor()

    def execute(self, query, query_name=None, sync_ddl=False, db=None):
        """
        Execute a query supplied by a task in the running TaskSet.

        Register Locust request_success and request_failure events.
        """
        # query_name is needed for grouping the stats in logs and CSV files.
        if query_name is None:
            query_name = query

        start_time = time.time()
        try:
            if db is not None:
                self.cursor.execute('use {db}'.format(db=db))
            if sync_ddl:
                with self.__enable_sync_ddl__():
                    self.cursor.execute(query)
            else:
                self.cursor.execute(query)
            response = self.cursor.fetchall()
        except ProgrammingError as e:
            # Some operations -- e.g. invalidate metadata -- properly don't
            # return a value even when successful, and will throw an exception
            # when fetchall() is called. This is not really a request_failure.
            response = []
        except Exception as e:
            # Any other errors count as legitimate query failures
            total_time = int((time.time() - start_time) * 1000)
            locust.events.request_failure.fire(
                request_type="query", name=query_name,
                response_time=total_time, exception=e
            )
            raise

        total_time = int((time.time() - start_time) * 1000)
        locust.events.request_success.fire(
            request_type="query", name=query_name,
            response_time=total_time, response_length=len(response)
        )

        return response

    @contextmanager
    def __enable_sync_ddl__(self):
        """Set sync_ddl for a single query."""
        self.cursor.execute('set sync_ddl=True')
        yield
        self.cursor.execute('set sync_ddl=False')

    def __del__(self):
        """Clean up connection when object is garbage collected."""
        logger.info("Killing locust...")
        if self is not None:
            self.cursor.close()
            self.conn.close()


class ImpylaLocust(locust.Locust):
    """
    Abstract Locust class. Locust users in test files will inherit from this.

    Provides an ImpylaLocustClient that can be used to make Impala requests
    that will be tracked in Locust's statistics.
    """

    def __init__(self, *args, **kwargs):
        super(ImpylaLocust, self).__init__(*args, **kwargs)
        self.client = ImpylaLocustClient()


def setup_test_config(config_file=None, **kwargs):
    """
    Event handler to process the yaml config file, and get cluster nodes.

    This event handler is aded to the start_test EventHook for all tests,
    and should be fired once, just as the Locust test first starts.
    """
    logger.info('Setting up TestConfig')
    with open(config_file) as fh:
        TestConfig.config = yaml.load(fh)
        TestConfig.nodes = get_node_hostnames(TestConfig.config['cm_host'])


def setup_database(test_config_object=None, **kwargs):
    """
    If required, create the DB and table required for the running test.

    Assumes that the test_config_object has already been populated by the
    setup_test_config event handler.
    """
    logger.info('Setting up test databases and tables')
    tc = test_config_object
    test_db = tc.config['setup_database']['db']
    test_table = '{}.{}'.format(test_db, tc.config['setup_database']['table'])

    create_db_query = 'CREATE DATABASE IF NOT EXISTS {db}'.format(db=test_db)
    create_tbl_query = ('CREATE TABLE IF NOT EXISTS {tbl} (s string) PARTITIONED '
                        'BY (key1 int, key2 int)'.format(tbl=test_table))

    client = ImpylaLocustClient()
    client.connect(tc.nodes[0], hs2_port=tc.config['hs2_port'])

    for query in (create_db_query, create_tbl_query):
        client.execute(query, sync_ddl=True)


def get_node_hostnames(cm_host):
    """Return the list of hostnames of nodes managed by a given cm_host."""
    logger.info('Querying CM API for node hostnames')
    api = ApiResource(cm_host, username="admin", password="admin", version=15)
    hosts = api.get_all_hosts()
    return [host.hostname for host in hosts[1:]]  # Skip hosts[0], the CM itself


def parse_sql_file(sql_file):
    """Parse .sql file, and return formatted query as a string."""
    with open(sql_file) as fh:
        # Filter out comments, which are denoted by '--' in SQL files
        sql = ' '.join([line.split('--')[0].strip() for line in fh
                        if not line.startswith('--')])
    return sqlparse.format(sql, reindent=True, keyword_case='upper')


# 'start_test' is the event hook that individual tests can fire() when first
# starting up. Any arbitrary handler (callable) can be attached to an event
# hook. Upon firing, handlers are run in the order in which they are added.
#
# By default, we'll start by attaching setup_test_config.
start_test = locust.events.EventHook()
start_test += setup_test_config
