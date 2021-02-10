#!/usr/bin/env python
"""
Setup for cpg package (installing analysis-runner CLI)
"""

import setuptools

PKG = 'analysis-runner'

setuptools.setup(
    name=PKG,
    version='0.1.2',
    description='Analysis runner to help make analysis results reproducible',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url=f'https://github.com/populationgenomics/{PKG}',
    license='MIT',
    packages=['cli'],
    include_package_data=True,
    zip_safe=False,
    entry_points={'console_scripts': ['cpg-analysisrunner=cli.cli:main']},
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
