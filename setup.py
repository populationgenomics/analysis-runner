#!/usr/bin/env python

"""
Setup script for the Python package.
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
    version='3.2.4',
    description='Analysis runner to help make analysis results reproducible',
    long_description=read_file('README.md'),
    long_description_content_type='text/markdown',
    url=f'https://github.com/populationgenomics/{PKG}',
    license='MIT',
    packages=['analysis_runner'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'cloudpathlib[all]',
        'cpg-utils>=5.0.0',
        # Avoid dependency resolution backtracking caused by hail pinning an
        # old version of protobuf and recent versions of grpcio-status requiring
        # a much newer version
        'grpcio-status>=1.48,<1.50',
        'hail>=0.2.134',
        'requests',
        'tabulate',
    ],
    entry_points={
        'console_scripts': ['analysis-runner=analysis_runner.cli:main_from_args'],
    },
    keywords='bioinformatics',
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
    ],
)
