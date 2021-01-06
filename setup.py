from setuptools import setup, find_packages


setup(
    name="flatten-athena-table",
    version="0.0.1",
    description="A tool for flattening a nested Athena table",
    url="https://github.com/skuroq/athena_schema_parser",
    install_requires=[
        "typer==0.3.2",
        "python-slugify",
        "loguru",
        "boto3",
        "lark-parser",
        "python-slugify",
        "sqlparse",
        "jinja2",
        "pyathena",

    ],
    entry_points={
        "console_scripts": ["flatten=flatten.cli:cli"],
    },
    packages=[
        "flatten"
    ],
    package_data={"": ["*.sql", "*.lark"]},
    extras_require={"test": ["pytest", "flake8"]},
)