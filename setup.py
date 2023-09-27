try:
    from setuptools import setup, find_packages  # type: ignore
except ImportError:
    from distutils.core import setup

config = {
    "description": "Python-Based Argo Dags and Operators",
    "author": "davidboren",
    "url": "http://www.github.com/davidboren/airgo",
    "download_url": "http://www.github.com/davidboren/airgo",
    "author_email": "dboren@g.ucla.edu",
    "version": "0.3.0",
    "install_requires": [
        "PyYaml>=5.1",
        "jinja2",
        "click",
        "croniter",
        "pytz",
        "cached_property",
    ],
    # Note boto3 constraint required for snowflake-connector-python for now
    "extras_require": {
        "snowflake": [
            "snowflake-connector-python>=2.0.0",
            "pandas",
            "sqlalchemy",
            "snowflake-sqlalchemy",
        ],
        "dropbox": ["snowflake-connector-python", "pandas", "dropbox"],
        "aws": ["boto3"],
    },
    "packages": find_packages(exclude=("tests", "docs")),
    "package_data": {"airgo": ["templates/*", "bin/*", "py.typed"]},
    "scripts": [],
    "name": "airgo",
    "entry_points": {"console_scripts": ["airgo = airgo.__main__:main"]},
    "zip_safe": False,
}
setup(**config)
