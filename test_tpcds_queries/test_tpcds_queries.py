"""
A simple SQL load test.

Randomly selects from a directory of SQL queries, and runs them concurrently
using the specified number of workers. The test expands to any number of SQL
files and any size cluster, evenly distributing queries to different impalads.

This test assumes that the TestConfig specifies a DB that already exists on
the cluster, and that the table names are coded into the SQL files directly.
"""
import itertools
import locust
import logging
import os
import random

from impala_loadtest.common import (
    ImpylaLocust,
    TestConfig,
    start_test,
    parse_sql_file
)

logger = logging.getLogger(name='SQL_Load_Test')
parent_dir = os.path.abspath(os.path.dirname(__file__))

# Setup the TestConfig object when Locust first starts
start_test.fire(config_file=os.path.join(parent_dir, 'tpcds_load_test.yaml'))
config = TestConfig.config


class RunSQLQueryFiles(locust.TaskSet):
    """Workload for running randomly selected SQL queries."""

    query_dir = os.path.join(parent_dir, 'queries')
    impalads = itertools.cycle(TestConfig.nodes)

    def on_start(self):
        """
        Connect to the next impalad in the list.

        The on_start handler is called once, when a Locust worker first
        hatches and before any tasks are scheduled.
        """
        self.client.connect(self.impalads.next(), hs2_port=config['hs2_port'])

    @locust.task
    def run_query(self):
        """Randomly select and run from a local directory of SQL queries."""
        sql_file = random.choice(os.listdir(self.query_dir))
        logger.debug("Running query: {sql_file}".format(sql_file=sql_file))

        # Even though the queries will go to various impalads, the stats
        # will group by only the filename of the SQL query file. To further
        # differentiate stats by which impalad is processing each query,
        # self.client.hostname can be added to the query_name here.
        self.client.execute(
            db=config['target_db'],
            query=parse_sql_file(os.path.join(self.query_dir, sql_file)),
            query_name=sql_file.split('.')[0]  # e.g., q8, q19, q27, etc.
        )


class ImpalaUser(ImpylaLocust):
    """A worker capable of executing the given task set."""

    min_wait = config['min_wait']   # Wait time in milliseconds
    max_wait = config['max_wait']
    task_set = RunSQLQueryFiles
