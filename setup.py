#!/usr/bin/env python

import io

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

from hurraypy import __version__

with io.open('README.rst', encoding='utf-8') as f:
    long_description = f.read()


setup(
    name='hurraypy',
    version=__version__,
    description='Python client for Hurray h5py',
    long_description=long_description,
    url='https://github.com/meteotest/hurray-py',
    author='Meteotest',
    author_email='remo.goetschi@meteotest.ch',
    maintainer='Reto Aebersold',
    maintainer_email='aeby@substyle.ch',
    keywords=['h5py', ],
    license='BSD',
    packages=['hurraypy'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: BSD License',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
    ],
    install_requires=[
        'numpy>=1.6.1',
        'msgpack-python>=0.4.8'
    ],
    extras_require={
        "dev": [
            "sphinx==1.6.1",
            "sphinx-rtd-theme==0.2.4",
            "jupyter>=1.0.0",
            # Note that nbsphinx requires an installation of Pandoc
            # http://pandoc.org/installing.html
            "nbsphinx==0.2.14",
        ],
    },
    test_suite='tests.get_tests'
)
