#!/usr/bin/env python

"""
Setup script for the Python package
- Used for development setup with `pip install --editable .`
- Parsed by conda-build to extact version and metainfo
"""

import setuptools

PKG = 'analysis-runner'

setuptools.setup(
    name=PKG,
    # This tag is automatically updated by bump2version
    version='1.1.0',
    description='Analysis runner to help make analysis results reproducible',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url=f'https://github.com/populationgenomics/{PKG}',
    license='MIT',
    packages=['cli'],
    include_package_data=True,
    zip_safe=False,
    entry_points={'console_scripts': ['analysis-runner=cli.cli:main']},
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
