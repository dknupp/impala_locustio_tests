# Impala Load Tests with Locust.io: Proof of Concept

## Why Locust.io?

* Purely code-based
  * easier to submit tests for peer code review
  * easier to write load tests with complex programming logic
  * easier to debug tests
* It's just Python
  * doesn't require juggling various file types and contexts (e.g., JMeter automation
    often combines XML, BeanShell, CSV, plus a Python wrapper)
  * easily incorporated into existing Python-based test automation frameworks
  * deloyable with nothing more than virtualenv and pip
* Based on async coroutines, rather than OS threads, so might be less resource intensive
  on the worker being used to generate the load

## Sample Load Tests

The most basic example of a Locust test file defines a worker class, and a
taskset containing one or more tasks that each worker instance will execute.
More exotic tests might define multiple types of workers, nested tasksets,
or run in a distrbuted master/slave configuration.

The sample tests in this repo follow the basic model of a single type of
worker that knows how to execute tasks from a single taskset.

* _test_tpcds_load_

  The directory contains the locust file, two sample YAML config files, and a
  directory of query files along with expected results for different scale sizes.
  It's assumed that the data is already loaded. The test will run until manually
  stopped, or for the given length of time specified if running in batch mode.

  [Note: it's been found that some of the queries seem to return non-deterministic
  results, so these query files have been moved to another directory.]

  The taskset concurrently contains three tasks:

  * Execute randomly-selected queries from a directory of TPC-DS queries.

  * In addition to simply running queries, the test will periodically validate
    the results received from the query.

  * Occasionally, each worker will disconnect and reconnect.

* _test_dwx_basic_

  The taskset concurrently consists of a single task that arbitrarily runs
  a query against the default airline dataset on a DWX warehouse.

  The directory contains the locust file, and a sample YAML config file.


* _test_tpch_load_

  Like the TPCDS tests, except there's no task for validating query results.


* _test_dwx_basic_

  The taskset concurrently consists of a single task that arbitrarily runs
  a query against the default airline dataset on a DWX warehouse.

  The directory contains the locust file, and a sample YAML config file.


* _test_tpcds_throughput_

  Simimlar to _test_tpcds_load_ test, but concurrent workers are coordinated
  the same query at the same time. As a group, they traverse the TPCDS suite
  in order.


## Common Code

* _impala_loadtest_

  Contains the necessary shared code in a python library, such as

  * DbApiLocustClient, a Locust wrapper around the DBAPI client
  * TestConfig
  * several helper funtions
  * etc.

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
(locust_env) $ pip install -e impala_locustio_tests/
```

You will also need to install the QE Client Library: https://github.infra.cloudera.com/dknupp/qe-client-lib

## Running tests

Locust tests can be run in two modes: either with a web UI or without. Running
with the UI is probably the easiest for getting started, whereas ```no-web```
mode would be the best for automated test runs.

### Running tests with the web UI

For example, to run the test_tpcds_queries test with the web UI:

```
(locust_env) $ CONFIG=<path to config file> locust -f test_tpcds_queries/test_tpcds_queries.py
```

Open a web browser to http://localhost:8089. In the form, you can specify the
number of concurrent workers, and the rate at which to spawn new workers until
the desired count is acheived. Then click the __Start Swarming__ button. Once
the number of workers have fully ramped up, the stats get zeroed out, and the
test can be considered running. You should be able to confirm this in the CM UI,
the Impala Debug UI, the Grafana stats page for a DWX warehouse.

The Locust web UI will show basic stats, a few charts showing running traffic,
and provides some links for downloading basic stats as a CSV file.

### Running tests in ```no-web``` mode

When running in ```no-web``` mode, the most important parameters (aside from
```no-web```) are:

* ```-c```: the number or workers (or "locusts")
* ```-r```: the rate at which to spawn workers per second
* ```--run-time```: the length of time to run the test, e.g., 1h30m
* ```--csv```: a base name for the .csv files that will be saved.
  If not specified, no csv output will be produced.

```
(locust_env) $ CONFIG=dwx3_tpcds_load_test.yaml locust -f test_tpcds_load.py -c 5 -r 2 --no-web --run-time 5m --csv dwx_load_test
```

To see the full list of Locust command line options

```
(locust_env) $ locust --help
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
