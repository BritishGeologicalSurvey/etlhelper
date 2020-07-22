"""setup.py

Setup script for the etlhelper library

Notes
-----

- Replaces the need for requirements.txt


"""
import pathlib
from setuptools import setup
import versioneer

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="etlhelper",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="A Python library to simplify data transfer between databases.",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/BritishGeologicalSurvey/etlhelper",
    author="BGS Informatics",
    author_email="jostev@bgs.ac.uk",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: GIS",
    ],
    python_requires='>=3.6',
    packages=["etlhelper", "etlhelper.db_helpers"],
    include_package_data=True,
    install_requires=[],
    extras_require={
        'dev': ['flake8',
                'ipdb',
                'ipython',
                'pytest',
                'pytest-cov',
                'versioneer'
                ],
        'oracle': ['cx-oracle'],
        'mssql': ['pyodbc'],
        'postgres': ['psycopg2-binary']},
    entry_points={
        "console_scripts": [
            "setup_oracle_client=etlhelper.setup_oracle_client:main",
        ]
    },
)
