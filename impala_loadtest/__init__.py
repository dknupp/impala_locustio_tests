"""Primary Locust classes"""

import locust
import logging
import sys
import time
import yaml

import qe_client_lib.dbapi_clients as client_lib

logging.basicConfig()
logger = logging.getLogger(name='impala_loadtest')

# Reduces the chattiness of hiveserver2 logging
logging.getLogger('impala.hiveserver2').setLevel(logging.WARNING)

TestConfig = {}


class DbApiLocustClient(object):
  """
  A proxy class for a DBAPI client that can emit Locust success/failure events.
  """

  def hatch(self, host, client_type="ImpylaClient", **client_kwargs):
    """
    Args:
      hostname: FQDN to the node under test
      client_type: name of the DBAPI client class to instantiate
      client_kwargs: a dictionary of parameters needed to make a connection
    """
    ClientType = getattr(client_lib, client_type)
    self._dbapi_client = ClientType(host, **client_kwargs)

  def logged_query(self, query_str, query_name=None, return_response=False):
    """
    A wrapper around the native .query() method of the underlying DBAPI client.

    This will log locust success or failure events for each underlying query.

    Args:
      query_str: the query to execute
      query_name: a string used to identify the query in the reported results
        (including the Locust UI, console output, and csv files).
      return_response: Boolean to determine whether DB results should be
        returned to the caller

    Returns:
      response from the query if return_response==True
    """
    if query_name is None:
      query_name = query_str

    start_time = time.time()
    try:
      response = self._dbapi_client.query(query_str)
    except Exception as e:
      # Note that this will report a failure to Locust, but will not
      # halt the test
      total_time = int((time.time() - start_time) * 1000)
      locust.events.request_failure.fire(
        request_type="query", name=query_name,
        response_time=total_time, response_length=len(str(e)),
        exception=e
      )
      raise

    total_time = int((time.time() - start_time) * 1000)
    locust.events.request_success.fire(
      request_type="query", name=query_name,
      response_time=total_time, response_length=sys.getsizeof(response)
    )

    if return_response:
      return response

  def __getattr__(self, name):
    """Proxy all other calls through to underlying DBAPI client"""
    return getattr(self._dbapi_client, name)


class DbApiLocust(locust.Locust):
  """
  Abstract Locust base class. Locust users will inherit from this.

  Contains a DbApiLocustClient that can be used to make Impala requests
  that will be tracked in Locust's statistics.
  """
  def __init__(self, *args, **kwargs):
    super(DbApiLocust, self).__init__(*args, **kwargs)
    self.client = DbApiLocustClient()


def setup_test_config(config_file=None, **kwargs):
  """
  Event handler to process the yaml config file.

  This event handler is added to the test_setup EventHook, and should be
  fired once at the top of locust file that needs to parse params from
  a config file.
  """
  logger.info('Parsing TestConfig: {0}'.format(config_file))
  with open(config_file) as fh:
    TestConfig.update(yaml.safe_load(fh))


# 'test_setup' is the event hook that individual tests can fire() when first
# starting up. Any arbitrary handler (callable) can be attached to an event
# hook. Upon firing, handlers are run in the order in which they are added.
#
# By default, we'll start by attaching setup_test_config.
test_setup = locust.events.EventHook()
test_setup += setup_test_config
