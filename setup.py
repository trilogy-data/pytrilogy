# type: ignore
import ast
import re

import setuptools

_version_re = re.compile(r"__version__\s+=\s+(.*)")
with open("trilogy/__init__.py", "rb") as f:
    _match = _version_re.search(f.read().decode("utf-8"))
    if _match is None:
        print("No version found")
        raise SystemExit(1)
    version = str(ast.literal_eval(_match.group(1)))


with open("requirements.txt", "r") as f:
    install_requires = [line.strip().replace("==", ">=") for line in f.readlines()]

setuptools.setup(
    name="pytrilogy",
    version=version,
    url="",
    author="",
    author_email="preql-community@gmail.com",
    description="Declarative, typed query language that compiles to SQL.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(
        exclude=[
            "dist",
            "build",
            "*.tests",
            "*.tests.*",
            "tests.*",
            "tests",
            "docs",
            ".github",
            "",
            "examples",
        ]
    ),
    package_data={
        "": ["*.tf", "*.jinja", "py.typed", "*.lark", "*.preql"],
    },
    install_requires=install_requires,
    extras_require={
        "postgres": ["psycopg2-binary"],
        "bigquery": ["sqlalchemy-bigquery"],
        "snowflake": ["snowflake-sqlalchemy"],
    },
    entry_points={
        "console_scripts": ["trilogy=trilogy.scripts.trilogy:cli"],
    },
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)
