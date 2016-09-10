#!/usr/bin/env python
import os

from hurraypy import __version__

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

f = open(os.path.join(os.path.dirname(__file__), 'README.rst'))
long_description = f.read()
f.close()

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
    license='MIT',
    packages=['hurraypy'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
    ]
)
