#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
      name="tap-fastly",
      version="0.1.0",
      description="Singer.io tap for extracting Fastly billing data.",
      author="SplitIO",
      url="http://split.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_fastly"],
      install_requires=[
            "singer-python>=5.0.12",
            "requests",
            "pendulum"
      ],
      entry_points="""
            [console_scripts]
            tap-fastly=tap_fastly:main
      """,
      packages=find_packages(),
      package_data={
          'tap_fastly': [
              'schemas/*.json'
          ]
      }
)
