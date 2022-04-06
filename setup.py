#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='commons',
      version='1.0',
      description='Common libraries for taps/targets',
      long_description_content_type='text/markdown',
      author='Inditex',
      install_requires=[
          'pycryptodome==3.14.1'
      ],
      entry_points='''
          [console_scripts]
      ''',
      packages=find_packages()
)
