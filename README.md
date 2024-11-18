### Introduction
This is a sample project to demonstrate the use of `Great Expectations` to validate and document data quality.
This example uses a sample transaction data set converting to `Pandas` DataFrame and then validate. It will automatically generate data documentation in HTML format and store the scanned result to postgres database.

### Installation
To install the project, follow the steps below:
1. Clone the repository
2. Create a virtual environment using `python -m venv venv`
3. Activate the virtual environment using `source venv/bin/activate` or `venv\Scripts\activate` on Windows
4. Install the required packages using `pip install -r requirements.txt`
5. Copy `.env-example` to `.env` and update the values as per your environment.

### Running the project
To run the project, follow the steps below:
1. Initialize Great Expectations using `python init.py`
2. Run the validation using `python main.py`
3. To recreate the ge run: `python init.py --mode recreate`
### Understanding the project
The project consists of two files:
1. `init.py`: This file initializes Great Expectations and creates the data context.
2. `main.py`: This file runs the validation and generates the data documentation.
3. `data`: This folder contains the sample data to be validated.

### Activate pre-commit hook
Install `pre-commit` by running
`pip install pre-commit` and then run `pre-commit install` to install the hooks.
