from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path
import pip

here = path.abspath(path.dirname(__file__))

setup(
    name='babel_datapipeline',

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version='1.0.0',

    description='Datapipeline from raw files on S3 to recommends in Dynamo',

    # The project's main homepage.
    url='https://github.com/iwsmith/babel_datapipeline',

    # Author details
    author='The Babel Development Team',
    author_email='babel_platform_discuss@googlegroups.com',

    # Choose your license
    license='GNU Affero General Public License',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Information Analysis',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ],

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(exclude=['', 'debug']),

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=['scipy', 'networkx', 'boto', 'configobj', 'luigi', 'babel_util'],
)

with open('requirements.txt', 'r') as f:
    for line in f:
        pip.main(['install', line])