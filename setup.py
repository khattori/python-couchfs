# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

setup(
    name='python-couchfs',
    version='0.0.1',
    description='CouchFS',
    author='Fixpoint, Inc.',
    author_email='info@fixpoint.co.jp',
    packages=find_packages(),
    install_requires=[
        "decorator ~= 4.0.10",
        "pytz",
        "python-dateutil ~= 2.6.0",
        "fuse-python ~= 0.2.0",
        "CouchDB ~= 1.1"
    ]
)
