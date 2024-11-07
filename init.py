import os
import shutil
import great_expectations as gx


class Initiator:
    """Initialize the Great Expectations context and add data assets, suites, and validation definitions."""

    PROJECT_DIR = "./gx"
    SOURCE_NAME = "pandas"
    ASSET_NAME = "transactions"
    BATCH_NAME = "transactions batch"

    @classmethod
    def initialize(cls) -> None:
        """Initialize the Great Expectations context."""
        # Check if the project directory exists, and delete it if it does
        if os.path.exists(cls.PROJECT_DIR):
            shutil.rmtree(cls.PROJECT_DIR)  # Delete the directory and all its contents
        cls.context = gx.get_context(mode="file", project_root_dir=Initiator.PROJECT_DIR)
        cls.context.enable_analytics(enable=False)
        cls.add_data_assets()
        cls.add_suites_and_validation_definitions()
        cls.add_checkpoint()

    @classmethod
    def add_data_assets(cls) -> None:
        """Add data assets, suites, and validation definitions."""
        # Add a pandas datasource
        data_source = cls.context.data_sources.add_pandas(name=Initiator.SOURCE_NAME)

        # Add a DataFrame asset for transactions
        data_asset = data_source.add_dataframe_asset(name=Initiator.ASSET_NAME)

        # Add a Batch Definition to the Data Asset
        cls.batch_definition = data_asset.add_batch_definition_whole_dataframe(
            Initiator.BATCH_NAME,
        )

    @classmethod
    def add_suites_and_validation_definitions(cls) -> None:
        """Add suites and validation definitions to the data context."""
        cls.distribution_suite()
        cls.missingness_suite()
        cls.schema_suite()
        cls.volume_suite()

    @classmethod
    def distribution_suite(cls) -> None:
        """Add expectation suite to the data context."""
        # Define an expectation suite
        suite = cls.context.suites.add(gx.ExpectationSuite(name="distribution"))

        # Add expectations to the suite
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(column="transaction_id", min_value=1001, max_value=1020),
        )
        # Create a Validation Definition and Add to the Data Context
        cls.context.validation_definitions.add(gx.ValidationDefinition(
            data=cls.batch_definition, suite=suite, name="distribution",
        ))

    @classmethod
    def missingness_suite(cls) -> None:
        """Define an expectation suite."""
        suite = cls.context.suites.add(gx.ExpectationSuite(name="missingness"))

        # Add expectations to the suite
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"),
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeNull(column="product_rating", mostly=0.8),
        )

        # Create a Validation Definition and Add to the Data Context
        cls.context.validation_definitions.add(gx.ValidationDefinition(
            data=cls.batch_definition, suite=suite, name="missingness",
        ))

    @classmethod
    def schema_suite(cls) -> None:
        """Define an expectation suite."""
        suite = cls.context.suites.add(gx.ExpectationSuite(name="schema"))

        # Add expectations to the suite
        suite.add_expectation(
            gx.expectations.ExpectColumnToExist(column="purchase_date"),
        )
        suite.add_expectation(
            gx.expectations.ExpectTableColumnCountToEqual(value=6),
        )

        # Create a Validation Definition and Add to the Data Contex
        cls.context.validation_definitions.add(gx.ValidationDefinition(
            data=cls.batch_definition, suite=suite, name="schema",
        ))

    @classmethod
    def volume_suite(cls) -> None:
        """Define volume expectation suite."""
        suite = cls.context.suites.add(gx.ExpectationSuite(name="volume"))

        # Add expectations to the suite
        suite.add_expectation(
            gx.expectations.ExpectTableRowCountToEqual(value=21),
        )

        # Create a Validation Definition and Add to the Data Context
        cls.context.validation_definitions.add(gx.ValidationDefinition(
            data=cls.batch_definition, suite=suite, name="volume",
        ))

    @classmethod
    def add_checkpoint(cls) -> None:
        """Add checkpoints to the data context."""
        cls.statistical_checkpoint()
        cls.completeness_checkpoint()

    @classmethod
    def statistical_checkpoint(cls) -> None:
        """Add checkpoint to the data context."""
        cls.context.checkpoints.add(gx.Checkpoint(
            name="statistical_checkpoint",
            validation_definitions=[
                cls.context.validation_definitions.get("distribution"),
            ],
        ))

    @classmethod
    def completeness_checkpoint(cls) -> None:
        """Add checkpoint to the data context."""
        cls.context.checkpoints.add(gx.Checkpoint(
            name="completeness_checkpoint",
            validation_definitions=[
                cls.context.validation_definitions.get("missingness"),
                cls.context.validation_definitions.get("schema"),
                cls.context.validation_definitions.get("volume"),
            ],
        ))


Initiator.initialize()
