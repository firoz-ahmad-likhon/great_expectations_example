from datetime import datetime, timezone
import great_expectations as gx
import pandas as pd
from great_expectations import RunIdentifier
import logging

# Create a logger and set the level
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def run() -> None:
    """Validate and generate static docs."""
    # Load the data into a DataFrame
    df = pd.read_csv('Data/transactions.csv')

    # Get the Great Expectations context
    context = gx.get_context(mode="file", project_root_dir="./gx")

    # Define validation definitions
    validation_definitions = {
        "distribution": context.validation_definitions.get("distribution"),
        "missingness": context.validation_definitions.get("missingness"),
        "schema": context.validation_definitions.get("schema"),
        "volume": context.validation_definitions.get("volume"),
    }

    # Initialize an empty dictionary to hold validation results
    validation_results = {}

    # Define a unique run_id based on the current timestamp
    run_id = RunIdentifier(run_name="Quality", run_time=datetime.now(tz=timezone.utc).strftime('%Y%m%dT%H%M%S.%f'))

    # Run validations for each definition
    for name, validation_definition in validation_definitions.items():
        if validation_definition is not None:
            # Run the Validation Definition
            result = validation_definition.run(batch_parameters={"dataframe": df}, run_id=run_id)
            validation_results[name] = result
        else:
            logging.error(f"Validation definition '{name}' not found.")

    # Review the Validation Results
    logging.info(validation_results)

    # Build the Data Docs
    context.build_data_docs()
    # Print out the docs URL
    logging.info(f"Data Docs available at: {context.get_docs_sites_urls()[0]['site_url']}")

if __name__ == '__main__':
    run()
