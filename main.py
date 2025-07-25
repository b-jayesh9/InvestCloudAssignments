import logging
import sys
from config import PipelineConfig
from pipeline.orchestrator import EtlOrchestrator
from data_generator.generator import TestDataGenerator


def setup_logging():
    """Configures logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)-22s - %(levelname)-8s - %(message)s',
        stream=sys.stdout  # Log to standard out
    )


def main():
    """Main entry point for the ETL application."""
    setup_logging()
    config = PipelineConfig()

    # --- Step 1: Generate test data if it doesn't exist ---
    if not config.INPUT_DIR.exists() or not any(config.INPUT_DIR.iterdir()):
        logging.info("Test data not found. Generating new dataset...")
        generator = TestDataGenerator()
        # Define the dataset: 20 files, 1 GB each.
        # We can change it according to our test needs, I just added 20,1 for a quick test.
        # This will generate a small ~15-20 GB dataset for a quick test run.
        file_configs = [(20, 1)]
        generator.create_test_dataset(str(config.INPUT_DIR), file_configs)
        logging.info("Test data generation complete.")
    else:
        logging.info(f"Using existing data from {config.INPUT_DIR}")

    # --- Step 2: Run the main ETL pipeline ---
    orchestrator = EtlOrchestrator(config)
    orchestrator.run()


if __name__ == "__main__":
    main()