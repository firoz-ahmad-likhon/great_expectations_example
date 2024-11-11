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

    run_id = RunIdentifier(run_name="Quality", run_time=datetime.now(tz=timezone.utc).strftime('%Y%m%dT%H%M%S.%f'))

    statistical_result = context.checkpoints.get("statistical_checkpoint").run(
        batch_parameters={"dataframe": df}, run_id=run_id,
    )

    completeness_result = context.checkpoints.get("completeness_checkpoint").run(
        batch_parameters={"dataframe": df}, run_id=run_id,
    )

    logging.info(f"Validation results: {statistical_result.success and completeness_result.success}")

    # Build the Data Docs
    context.build_data_docs()
    # Print out the docs URL
    logging.info(f"Data Docs available at: {context.get_docs_sites_urls()[0]['site_url']}")

    # todo: Add actions to checkpoints to trigger notifications and alerts
    # todo: Add another data source

if __name__ == '__main__':
    run()
