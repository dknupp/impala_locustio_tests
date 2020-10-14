"""Common classes and helpers for setting up a Locaust load test of Impala."""

import logging
import os
import re
import sqlparse
import yaml

from decimal import Decimal

logging.basicConfig()
logger = logging.getLogger(__file__)

# Full path to the impala_loadtest directory
LIB_DIR = os.path.dirname(os.path.abspath(__file__))


class Workloads(object):

  BASE_DIR = os.path.abspath(os.path.join(LIB_DIR, os.pardir, 'workloads'))

  @classmethod
  def get_workload_root(cls, workload_name):
    workload_root = os.path.join(cls.BASE_DIR, workload_name)
    assert os.path.exists(workload_root), "Invalid workload: {}".format(workload_name)
    return workload_root

  @classmethod
  def get_queries_directory(cls, workload_name):
    """
    """
    workload_root = cls.get_workload_root(workload_name)
    queries_dir = os.path.join(workload_root, 'queries')
    assert os.path.exists(queries_dir), "Invalid directory: {}".format(queries_dir)
    return queries_dir

  @classmethod
  def get_results_directory(cls, workload_name, scale):
    """
    """
    workload_root = cls.get_workload_root(workload_name)
    results_dir = os.path.join(workload_root, 'results', 'scale_{}'.format(scale))
    assert os.path.exists(results_dir), "Invalid directory: {}".format(results_dir)
    return results_dir


class DataTypeLoader(yaml.SafeLoader):
  """
  Yaml loader class with additional constructors for data type validation.
  (Still experimental.)
  """
  def construct_python_tuple(self, node):
    return tuple(self.construct_sequence(node))

  def construct_Decimal(self, node):
    value = node.value[0].value
    return Decimal(value)


# Add Yaml constructor for native python tuples
DataTypeLoader.add_constructor(
  u'tag:yaml.org,2002:python/tuple',
  DataTypeLoader.construct_python_tuple)

# Add Yaml constructor for python decimal.Decimal
DataTypeLoader.add_constructor(
  u'tag:yaml.org,2002:python/object/apply:decimal.Decimal',
  DataTypeLoader.construct_Decimal)


def get_node_hostnames(cm_host):
  """
  Utility function to get the node names managed by a given cm_host.

  This is quick and dirty, and assumes that the CM is always node[0].
  """
  # logger.info('Querying CM API for node hostnames')
  # api = ApiResource(cm_host, username="admin", password="admin", version=15)
  # hosts = api.get_all_hosts()
  # return [host.hostname for host in hosts[1:]]  # Skip hosts[0], the CM itself
  raise NotImplementedError("cm_api module is deprecated: need to use cm_client")


def parse_sql_file(sql_file):
  """
  Parse .sql file, and return formatted query as a string.

  Args:
    sql_file: ful path to a text file containing a single query
  """
  with open(sql_file) as fh:
    # Filter out comments, which are denoted by '--' in SQL files
    sql = ' '.join([line.split('--')[0].strip() for line in fh
                    if not line.startswith('--')])
  return sqlparse.format(sql, reindent=True, keyword_case='upper')


def parse_queries_from_file(sql_file, replace_strings=None):
  """
  Parse .sql file, and return queries as a list of strings.

  Args:
    sql_file: full path to a text file containing any number of queries,
      delineated by semi-colons.

    replace_strings: a dict in which the keys are regexes to replace in
      the final queries, and the values are the strings to replace any
      substring matching the regex.

  """
  with open(sql_file) as fh:
    # Filter out comments, which are denoted by '--' in SQL files
    file_contents = ' '.join([line.split('--')[0].strip() for line in fh
                              if not line.startswith('--')])

  raw_sql = sqlparse.format(file_contents, reindent=True, keyword_case='upper')

  if replace_strings is not None:
    for pattern, replacement in replace_strings.iteritems():
      raw_sql = re.sub(re.escape(pattern), replacement, raw_sql)
  return [q.strip() for q in raw_sql.split(';') if q.strip()]

