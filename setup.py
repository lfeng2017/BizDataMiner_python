# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

import os
import os.path as op
import subprocess

from dispatchETL import main as app

setup(name=app.APP_NAME,

      version=app.VERSION,

      url='git@git.yongche.org:lujin/BizDataMiner.git',

      license='MIT',

      author='lujin',

      author_email='lujin@yongche.com',

      description='Manage configuration files',

      packages=find_packages(exclude=['tests']),

      long_description=open('README.md').read(),

      zip_safe=False,

      setup_requires=['cement>=2.10.2'],

      test_suite='nose.collector')
