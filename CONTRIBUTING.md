
# Welcome to the Trilogy Contributing Guide <!-- omit in toc -->

Thank you for investing your time in contributing to our project! :.

Read our [Code of Conduct](./CODE_OF_CONDUCT.md) to keep our community approachable and respectable.

In this guide you will get an overview of the contribution workflow from opening an issue, creating a PR, reviewing, and merging the PR.

Use the table of contents icon <img src="/contributing/images/table-of-contents.png" width="25" height="25" /> on the top left corner of this document to get to a specific section of this guide quickly.

## New contributor guide

To get an overview of the project, read the [README](../README.md) file. Here are some resources to help you get started with open source contributions:

- [Finding ways to contribute to open source on GitHub](https://docs.github.com/en/get-started/exploring-projects-on-github/finding-ways-to-contribute-to-open-source-on-github)
- [Set up Git](https://docs.github.com/en/get-started/getting-started-with-git/set-up-git)
- [GitHub flow](https://docs.github.com/en/get-started/using-github/github-flow)
- [Collaborating with pull requests](https://docs.github.com/en/github/collaborating-with-pull-requests)


### Issues

#### Create a new issue

If you spot a problem with the docs, [search if an issue already exists](https://docs.github.com/en/github/searching-for-information-on-github/searching-on-github/searching-issues-and-pull-requests#search-by-the-title-body-or-comments). If a related issue doesn't exist, you can open a new issue.

#### Solve an issue

Scan through our [existing issues](https://github.com/trilogydata/pytrilogy/issues) to find one that interests you. You can narrow down the search using `labels` as filters. 


## Setting Up Your Environment

Recommend that you work in a virtual environment with requirements from both requirements.txt and requirements-test.txt installed. The latter is necessary to run
tests (surprise). 

## Running Tests

The tests are implemented primarily in pytest. To run all tests you are strongly suggested to have docker installed, though you can manually configured the required
data warehouse in an express edition of SQL server if docker is not possible. Guidance for the non-docker path is not provided. Docker is
STRONGLY RECOMMENDED.

A portion of the tests are dependent on having access to an AdventureWOrks2019DW example database
in Microsoft SQL Server that can be downloaded via this [link]https://github.com/Microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorksDW2019.bak.

The tests will treat this as database server a pytest fixture, starting a docker image if the tests detect a sql server is not already running. Before
you run tests you must build this docker image. From the root of this repository run the following to fetch the database data and build a docker image
containing it

```bash
/bin/bash ./docker/build_image.sh
```

If you are using windows download the AdventureWorks2019DW database backup from the link above and place it in the ./docker path.
From the root of the repo run
```bash
docker build --no-cache ./docker/ -t pyreql-test-sqlserver
```

To run the test suite, from the root of the repository run

```python
python -m pytest ./tests
```