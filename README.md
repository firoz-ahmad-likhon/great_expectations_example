## Introduction
This is a sample project to demonstrate the use of `Great Expectations` to validate and document data quality.
This example uses a sample transaction data set converting to `Pandas` DataFrame and then validate. It will automatically generate data documentation in HTML format and store the scanned result to postgres database.

The official documentation for Great Expectations can be found at [Official website](https://docs.greatexpectations.io/docs/home/) and the glossary of terms can be found at [Glossary](https://docs.greatexpectations.io/docs/reference/learn/glossary).

## Pre-requisites
1. A Postgres database to save the scanned result.

## Installation
To install the project, follow the steps below:
1. Clone the repository
2. Create a virtual environment using `python -m venv venv`
3. Activate the virtual environment using `source venv/bin/activate` or `venv\Scripts\activate` on Windows
4. Install the required packages using `pip install -r requirements.txt`
5. Copy `.env-example` to `.env` and update the values as per your environment.

## Running the project
To run the project, follow the steps below:
1. Initialize Great Expectations using `python init.py`
2. Run the validation using `python main.py`
3. To recreate once the init.py file is modified, run: `python init.py --mode recreate`

## Understanding the project
The project consists of two files:
1. `init.py`: This file initializes Great Expectations and creates the data context along with various configurations and rules.
2. `main.py`: This file scans the rules.
3. `data`: This folder contains the sample data to be validated.

### Type Checking and Linting
This repo uses `pre-commit` hooks to check type and linting before committing the code.

Install `pre-commit` by running `pip install pre-commit` and then run `pre-commit install` to install the hooks.

Perform below commands to:
1. Type Checking
`mypy . --pdb`
2. Linting
`ruff check .`


### Testing
To run the tests, run `pytest` in the terminal.
The test contains the following:
1. Integration test on the context.
