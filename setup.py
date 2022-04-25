#!/usr/bin/env python
from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

with open('requirements.txt') as f:
    requirements = f.read().splitlines()
    
setup(
    name='datajoint_extensions',
    version='1.0.0',
    description='Various extensions to the Datajoint library developed within the Moser group',
    author='Simon Ball',
    author_email='simon.ball@ntnu.no',
    license='MIT',
    url='https://github.com/kavli-ntnu/datajoint-extensions',
    packages=find_packages(exclude=[]),
    install_requires=requirements,
    python_requires='>=3.6',
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "Topic :: Datajoint"
    ]
)
