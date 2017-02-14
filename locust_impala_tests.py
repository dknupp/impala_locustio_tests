import argparse
import impala.dbapi
import random
import time
from cm_api.api_client import ApiResource
from impala.error import ProgrammingError
from locust import Locust, events, task, TaskSet

DEFAULT_HS2_PORT = 21050
# DEFAULT_CM_HOST = "impala-ubuntu1604-test-cluster-1.vpc.cloudera.com"
DEFAULT_CM_HOST = "vc0726.halxg.cloudera.com"


def parse_args():
    raise NotImplemented
    parser = argparse.ArgumentParser()
    args = parser.add_mutually_exclusive_group(required=True)
    args.add_argument("--cm-host", default=None,
                        help=("CM host name, e.g. 'my_cluster-1.foo.com'"))
    return parser.parse_args()


def get_nodes(cm_host):
    """Return nodes, given cm_host.

    Args:
        cm_host, e.g. my_cluster-1.vpc.cloudera.com

    Return a list of node hostnames:
        ['my_cluster-2.vpc.cloudera.com', 'my_cluster-3.vpc.cloudera.com', ...]
    """
    api = ApiResource(cm_host, username="admin", password="admin", version=15)
    hosts = api.get_all_hosts()
    return [host.hostname for host in hosts[1:]] # Skips CM master -- hosts[0]


class ImpalaLocustClient(object):
    """
    Simple, sample Impala DBAPI client implementation that fires locust events
    on request_success and request_failure, so that all requests get tracked
    in locust's statistics.
    """
    def __init__(self, impalad_host, hiveserver2_port):
        self.impalad_host = impalad_host
        self.hs2_port = hiveserver2_port

    def connect(self):
        self.conn = impala.dbapi.connect(host=self.impalad_host,
                                         port=self.hs2_port)
        self.cursor = self.conn.cursor()

    def execute_query(self, query):
        start_time = time.time()
        try:
            self.cursor.execute(query)
            try:
                response = self.cursor.fetchall()
            except ProgrammingError as e:
                # Some operations -- e.g. invalidate metadata -- don't return
                # a value even when successful, and will throw an exception
                # when fetchall() is called, but this should not be counted as
                # an error.
                response = ''
        except Exception as e:
            # This is a legitimate query exception.
            total_time = int((time.time() - start_time) * 1000)
            print str(e)
            events.request_failure.fire(request_type="query", name=query,
                                        response_time=total_time, exception=e)
        else:
            total_time = int((time.time() - start_time) * 1000)
            events.request_success.fire(request_type="query", name=query,
                                        response_time=total_time,
                                        response_length=len(response))
            return response

    def __del__(self):
        self.cursor.close()
        self.conn.close()


class ImpalaLocust(Locust):
    """
    This is the abstract Locust class. It provides an Impala client that can
    be used to make Impala requests that will be tracked in Locust's statistics.
    """
    def __init__(self, *args, **kwargs):
        super(ImpalaLocust, self).__init__(*args, **kwargs)
        self.client = ImpalaLocustClient(random.choice(self.impalads),
                                         self.port)


class AddingPartitionsTaskSet(TaskSet):
    def on_start(self):
        """Called when a Locust starts, before any task is scheduled."""
        self.connect_to_impalad()

    def connect_to_impalad(self):
        self.client.connect()

    @task
    def add_tables(self):
        query = ("create table functional.temp_table3 (s string) "
                 "partitioned by (month int, day int);")
        self.client.execute_query(query)


class BasicUserTasks(TaskSet):
    def on_start(self):
        """Called when a Locust starts, before any task is scheduled."""
        self.connect_to_impalad()

    def connect_to_impalad(self):
        self.client.connect()

    @task(5)
    def get_databases(self):
        self.client.execute_query('show databases')

    # @task(1)
    # def invalidate_table_metadata(self):
    #     query = 'invalidate metadata functional_avro.zipcode_incomes'
    #     self.client.execute_query(query)

    @task(5)
    def select_from_table_with_predicate(self):
        query = ("select * from functional_avro.zipcode_incomes "
                 "where zip > '99000'")
        self.client.execute_query(query)


class ImpalaUser(ImpalaLocust):
    impalads = get_nodes(DEFAULT_CM_HOST)
    port = DEFAULT_HS2_PORT

    min_wait = 2000
    max_wait = 6000
    task_set = BasicUserTasks