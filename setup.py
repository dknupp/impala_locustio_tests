#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function, division

import sys


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


def parse_requirements(requirements_file='requirements.txt'):
    """Parse requirements from a requirements file, stripping comments."""
    lines = []
    for req in open(requirements_file):
        # Skip options
        if req.startswith("-"):
            continue
        line = req.split('#')[0]
        if line.strip():
            lines.append(line)
    return lines


setup(
    name='impala_loadtest',
    version='0.0.1',
    description='Impala Load Test Common Modules',
    long_description=open('README.md').read(),
    author='Impala Dev Team',
    author_email='dev@cloudera.com',
    packages=['impala_loadtest'],
    install_requires=parse_requirements(),
)
