#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function, division

import pip
import sys

from pip.req import parse_requirements

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

def requirements():
    return [str(ir.req) for ir in parse_requirements('requirements.txt',
                                                     session=False)]

setup(
    name='impala_loadtest',
    version='0.0.1',
    description='Impala Load Test Common Modules',
    long_description=open('README.md').read(),
    author='Impala Dev Team',
    author_email='dev@cloudera.com',
    packages=['impala_loadtest'],
    install_requires=requirements(),
)
