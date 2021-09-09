#!/usr/bin/env python

"""
Setup script for the Python package
- Used for development setup with `pip install --editable .`
- Parsed by conda-build to extract version and metainfo
"""

import setuptools

PKG = 'analysis-runner'


def read_file(filename: str) -> str:
    """Returns the full contents of the given file."""
    with open(filename, encoding='utf-8') as f:
        return f.read()


setuptools.setup(
    name=PKG,
    # This tag is automatically updated by bump2version
    version='2.5.3',
    description='Analysis runner to help make analysis results reproducible',
    long_description=read_file('README.md'),
    long_description_content_type='text/markdown',
    url=f'https://github.com/populationgenomics/{PKG}',
    license='MIT',
    packages=['analysis_runner'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'console_scripts': ['analysis-runner=analysis_runner.cli:main_from_args']
    },
    keywords='bioinformatics',
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Natural Language :: English',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
    ],
)
