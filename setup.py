from setuptools import setup


setup(
    name="flatten-athena-table",
    version="0.0.1",
    description="A tool for flattening a nested Athena table",
    python_requires=">3.8.0",
    install_requires=[
        "typer",
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
    packages=["flatten"],
    package_data={"": ["*.sql", "*.lark"]},
    extras_require={"test": ["pytest", "flake8"]},
)

import setuptools

if __name__ == "__main__":
    setuptools.setup()
