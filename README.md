# Impala Load Tests with Locust.io: Proof of Concept

```
$ git clone git@github.mtv.cloudera.com:dknupp/impala_locustio_tests.git
```

## Repo Contents

The most basic example of a Locust test file defines a worker class, and a
taskset containing one or more tasks that each worker instance will execute.
More exotic tests might define multiple types of workers, nested tasksets,
or run in a distrbuted master/slave configuration.

Both of the sample tests in this repo follow the basic model of a single type
of worker that knows how to execute tasks from a single taskset. At this stage,
the tests simply generate load, without asserting passing or failing conditions.

* _test_tpcds_queries_

  This test concurrently executes randomly-selected queries from a directory of
  SQL files. The queries are distributed evenly among the nodes of a CM
  controlled cluster. It assumes that the database already exists, and that the
  table names are hard-coded in the SQL files. In the given example, we're
  using the ```tpcds``` database, and well-known TPCDS queries. The test will
  run until manually stopped, or for the given number of query executions
  if specified.

  The directory contains the test file, YAML config file, and SQL query files.

* _test_add_partition_drop_database_

  This slightly more complex test sets up a test database and a test table,
  than runs some number of concurrent workers to add partitions to the table.
  When the number of partitions reaches the limit specified in the test config
  file, the database is dropped. We expect to see some number of exceptions
  occur at that point.

  The directory contains the test file, and a YAML config file.

* _impala_loadtest_

  Contains the common code in a single module, ```common.py```, including

  * TestConfig
  * ImpylaLocustClient, a Locust wrapper around the Impyla client
  * the ImpylaLocust base class (worker)
  * several helper funtions

## Installation

### Setting up the virtualenv

You can install the packages directly into your system python environment, or
into a virtualenv that you can ```activate```, and then ```deactivate``` when
you're done, to drop back to the system python environment. I generally prefer
to work with a virtualenv.

If you decide to use a virtualenv, call ```virtualenv``` to create it anywhere
you have write access, and name it whatever you like. In the example below, the
virtualenv is simply called ```locust_env```.

```
$ virtualenv ~/my_virtualenvs/locust_env
New python executable in /home/username/my_virtualenvs/locust_env
Installing setuptools, pip, wheel...done.
```

To activate the virtualenv, source the ```activate``` script in the
```locust_env/bin/``` directory within the virtual environment. Once the
environment is active, you should see its name added at the beginning of your
shell's prompt.

```
$ source ~/my_virtualenvs/locust_env/bin/activate
(locust_env) $
```

### Installing the impala_loadtest common code

The necessary common code is packaged in the ```impala_loadtest/``` directory.
With your virtualenv active, you can install it directly from the directory
using ```pip install -e```. External dependencies will be installed as well.

```
(locust_env) $ cd impala_locustio_tests
(locust_env) $ pip install -e impala_loadtest/
[...]
Successfully installed Flask-0.12 Jinja2-2.9.5 MarkupSafe-1.0 PyYAML-3.12 Werkzeug-0.12.1 bitarray-0.8.1 click-6.7 cm-api-14.0.0 gevent-1.1.1 greenlet-0.4.12 impala-loadtest impyla-0.14.0 itsdangerous-0.24 locustio-0.7.5 msgpack-python-0.4.8 pyzmq-16.0.2 readline-6.2.4.1 requests-2.13.0 sqlparse-0.2.3 thrift-0.9.3
```

## Running tests

You will need a CM-managed cluster. Before running any test, change the
```cm_host``` in the YAML config file to point to your cluster's CM.

Locust tests can be run in two modes: either with a web UI or without. Running
with the UI is probably the easiest for getting started, whereas ```no-web```
mode would be the best for automated test runs.

### Running tests with the web UI

For example, to run the test_tpcds_queries test with the web UI, invoke

```
(locust_env) $ locust -f test_tpcds_queries/test_tpcds_queries.py
```

Open a web browser to http://localhost:8089. In the form, you can specify the
number of concurrent workers, and the rate at which to spawn new workers until
the desired count is acheived. Then click the __Start Swarming__ button. Once
the number of workers have fully ramped up, the stats get zeroed out, and the
test can be considered running. You should be able to confirm this in the CM UI,
or the Impala Debug UI.

The Locust web UI will show basic stats, and provides some links for
downloading basic stats as a CSV file.

### Running tests in ```no-web``` mode

When running in ```no-web``` mode, the most important parameters (aside from
```no-web```) are:

* ```-c```: the number or workers (or "locusts")
* ```-r```: the rate at which to spawn workers per second
* ```-n```: the maximum number of queries to execute

```
(locust_env) $ locust --no-web -f test_tpcds_queries/test_tpcds_queries.py -c 50 -r 1 -n 5000 --print-stats
```

To see the full list of Locust command line options

```
(locust_env) $ locust -h
```

### Deactivate the virtualenv

Don't forget to ```deactivate``` your virtualenv when you're done testing, by
simply callng the ```deactivate``` command. This will return you to your system
python environment.

```
(locust_env) $ deactivate
$
```

## Documentation

### Locust

* [Project] (http://locust.io/)
* [Docs] (http://docs.locust.io/en/latest/index.html)
* [Source] (https://github.com/locustio)

### Gevent

Locust uses asychronous coroutines in place of native OS threads

* [Introduction] (http://www.gevent.org/intro.html)
* [Tutorial] (http://sdiehl.github.io/gevent-tutorial/)

### Third Party Guides

* [Google Cloud Platform: Distributed Load Testing Using Kubernetes] (https://cloud.google.com/solutions/distributed-load-testing-using-kubernetes)
