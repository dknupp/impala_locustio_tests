import os
import yaml
from qe_client_lib.common import JDBCDriverManager, ODBCDriverManager

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(CURRENT_DIR, 'locust_tpcds_concurrency_test_params.yaml')


def get_test_env():
  params = {
    'client_type': os.getenv('CLIENT_TYPE'),
    'auth_type': None,
    'ssl': False,
    'user': os.getenv('USERNAME'),
    'password': os.getenv('PASSWORD'),
    'min_wait': float(os.getenv('MIN_WAIT')),
    'max_wait': float(os.getenv('MAX_WAIT')),
    'target_db': os.getenv('TARGET_DB'),
    'warmup': False,
    'num_concurrent_workers': int(os.getenv('NUM_CONCURRENT_WORKERS')),
    'num_iterations': int(os.getenv('NUM_ITERATIONS')),
    'thrift_transport': False
  }

  if os.getenv('USE_HTTP') == 'true':
    params['thrift_transport'] = 'http'

  if os.getenv('USE_KERBEROS') == 'true':
    params['auth_type'] = 'kerberos'

  if os.getenv('USE_SSL') == 'true':
    params['ssl'] = True

  if os.getenv('WARMUP') == 'true':
    params['warmup'] = True

  return params


if __name__ == "__main__":
  params = get_test_env()

  if params['client_type'] == 'OdbcClient':
    ODBCDriverManager.check_driver_installation('impala')
    ODBCDriverManager.check_driver_installation('hive')

  if params['client_type'] == 'JdbcClient':
    JDBCDriverManager.check_driver_installation()

  with open(CONFIG_FILE, 'w') as outfile:
    yaml.dump(params, outfile, default_flow_style=False)
