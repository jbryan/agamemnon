import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open('README.rst').read()
CHANGES = open('CHANGES.txt').read()

requires = ['pycassa', 'thrift', 'setuptools','ordereddict','rdflib','rdfextras','python-dateutil==1.5']
tests_requires = requires + ['nose', 'mock']

setup(name='agamemnon',
      version='0.3.1.0',
      description='A graph database built on top of cassandra',
      long_description=README + "\n\n" + CHANGES,
      classifiers=[
        "Programming Language :: Python",
        ],
      author='Tom Howe',
      author_email='trhowe@ci.uchicago.edu',
      url='https://github.com/globusonline/agamemnon',
      scripts=['bin/generate_indices'],
      license='LICENSE.txt',
      keywords='cassandra',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      install_requires=requires,
      tests_require=tests_requires,
      test_suite="nose.collector",
      )

