"""
Sample load test for running ALTER TABLE <t> ADD PARTITION.

This test sets up a test database and a test table, than runs some number
of concurrent workers to add partitions to the table. When the number of
partitions reaches the limit specified in the test config file, the database
is dropped. We expect to see some number of exceptions occur at that point.

Test table created as:
CREATE TABLE locust_test_db.locust_test_table (s string)
PARTITIONED BY (key1 int, key2 int);

Query: describe locust_test_db.locust_test_table
+------+--------+---------+
| name | type   | comment |
+------+--------+---------+
| s    | string |         |
| key1 | int    |         |
| key2 | int    |         |
+------+--------+---------+
Fetched 3 row(s) in 3.87s
"""

import itertools
import locust
import logging
import math
import os
import time

from impala_loadtest.common import (
    ImpylaLocust,
    TestConfig,
    start_test,
    setup_database
)

logger = logging.getLogger(name='ImpalaUser')
parent_dir = os.path.abspath(os.path.dirname(__file__))

# This test also requires the setup_database handler before tests are run.
start_test += setup_database
start_test.fire(
    config_file=os.path.join(parent_dir, 'add_partition_drop_db.yaml'),
    test_config_object=TestConfig
)

config = TestConfig.config


class AddPartitionsThenDropDatabase(locust.TaskSet):
    """
    Class representing a workload composed of two tasks.

    Task 1: Add a partition to a table
    Task 2: When the configured limit has been reached, drop the database
    """

    db_name = config['setup_database']['db']
    table = '{}.{}'.format(db_name, config['setup_database']['table'])

    alter_tbl_query = config['add_partition_task']['query'].format(tbl=table)
    drop_db_query = config['drop_database_task']['query'].format(db=db_name)
    impalads = itertools.cycle(TestConfig.nodes)

    def on_start(self):
        """
        Connect to the next impalad in the list.

        The on_start handler is called once, when a Locust worker first starts
        before any tasks are scheduled.
        """
        self.client.connect(self.impalads.next(), hs2_port=config['hs2_port'])

    @locust.task(config['add_partition_task']['frequency'])
    def add_partition(self):
        """
        ALTER TABLE {tbl} ADD PARTITION.

        The ratio of how many times this task executes in comparison to other
        tasks is defined by the frequency parameter.

        Raise StopLocust to halt all workers when an Exception is caught.
        (Several exceptions are expeced once the database has been dropped.)
        """
        # Generate unique keys for adding new partitions
        x, y = math.modf(time.time())
        keys = '(key1={key1}, key2={key2})'.format(key1=int(x * 100000000),
                                                   key2=int(y))

        try:
            self.client.execute(
                query='{0} {1}'.format(self.alter_tbl_query, keys),
                query_name='{0}: {1}'.format(self.client.hostname,
                                             self.alter_tbl_query)
            )
        except Exception as e:
            logger.error(str(e))
            raise locust.exception.StopLocust

    @locust.task(config['drop_database_task']['frequency'])
    def drop_database(self):
        """
        DROP DATABASE {db} CASCADE if partition count exceeds specified limit.

        The ratio of how many times this task executes in comparison to other
        tasks is defined by the frequency parameter.

        Raise StopLocust to halt all workers when an Exception is caught.
        """
        num_partitions = self._get_partitions_count(self.client.hostname)
        if num_partitions > config['partition_limit']:
            self.client.execute(
                query=self.drop_db_query,
                query_name='{0}: {1}'.format(self.client.hostname,
                                             self.drop_db_query)
            )

    def _get_partitions_count(self, hostname):
        """
        Parse the output of EXPLAIN SELECT count(*) to get number of partitions.

        This is a helper method used by the drop_database task. It also logs
        the current number of partitions in the table.
        """
        query = 'EXPLAIN SELECT count(*) FROM {tbl}'
        response = self.client.execute(
            query=query.format(tbl=self.table),
            query_name='{0}: {1}'.format(hostname, query)
        )

        # Response is a list of rows, with each row being a string in a tuple.
        # To find the count, we need to parse the row that looks like this:
        # ('   partitions=0/1590 files=0 size=0B',)
        for row in response:
            if 'partitions' in row[0]:
                num_partitions = int(row[0].split('/')[-1].split()[0])
                logger.info('num_partitions: {}'.format(num_partitions))
                return num_partitions


class ImpalaUser(ImpylaLocust):
    """A worker capable of executing the given task set."""

    min_wait = config['min_wait']   # Wait time in milliseconds
    max_wait = config['max_wait']
    task_set = AddPartitionsThenDropDatabase
