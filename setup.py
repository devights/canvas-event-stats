import os
from setuptools import setup

README = """
See the README 
"""

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

url = "https://github.com/uw-it-aca/regevents"
setup(
    name="regevents",
    version=1,
    packages=["regevents"],
    author="UW-IT T&LS",
    author_email="aca-it@uw.edu",
    include_package_data=True,
    install_requires=[
        "python-dateutil",
        "psycopg2==2.8.6",
        "sqlalchemy==2.0.28",
        "pandas==2.2.1",
    ],
    license="Apache License, Version 2.0",
    description="A tool for analyzing registration events",
    long_description=README,
    url=url,
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
    ],
)
