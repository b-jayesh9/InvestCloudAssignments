import time
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from tqdm import tqdm
import logging
import os

from config import PipelineConfig
from pipeline.preprocessor import FilePreprocessor
from pipeline.aggregator import SparkAggregator
from utils.metrics import PipelineMetrics

logger = logging.getLogger(__name__)


class EtlOrchestrator:
    """Orchestrates the hybrid ETL pipeline with polars and Pyspark."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.metrics = PipelineMetrics()

    def _get_dir_size_gb(self, path: Path) -> float:
        """Calculates the total size of a directory in gigabytes."""
        total_size = 0
        for dirpath, _, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)
        return total_size / (1024 ** 3)

    def _run_preprocessing_phase(self):
        """ Phase 1: Parallel file pre-processing using Polars."""
        logger.info("--- Starting Phase 1: Parallel Pre-processing with Polars ---")
        start_time = time.time()

        if self.config.INTERMEDIATE_DIR.exists():
            shutil.rmtree(self.config.INTERMEDIATE_DIR)
        self.config.INTERMEDIATE_DIR.mkdir(parents=True)
        time.sleep(10)  # Ensure directory is ready for writing
        input_files = list(self.config.INPUT_DIR.glob("*.csv"))
        if not input_files:
            raise FileNotFoundError(f"No CSV files found in {self.config.INPUT_DIR}")

        self.metrics.total_files_processed = len(input_files)
        self.metrics.total_input_gb = self._get_dir_size_gb(self.config.INPUT_DIR)

        preprocessor = FilePreprocessor()

        with ProcessPoolExecutor(max_workers=self.config.PREPROCESSING_WORKERS) as executor:
            futures = {
                executor.submit(
                    preprocessor.process_file,
                    in_path,
                    self.config.INTERMEDIATE_DIR / f"{in_path.stem}.parquet"
                ): in_path for in_path in input_files
            }
            for future in tqdm(as_completed(futures), total=len(futures), desc="Preprocessing files"):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"A subprocess failed for file {futures[future].name}: {e}")
                    raise

        self.metrics.preprocessing_time_sec = time.time() - start_time
        logger.info(f"--- Phase 1 Complete in {self.metrics.preprocessing_time_sec:.2f}s ---")

    def _run_aggregation_phase(self):
        """ Phase 2, instantiates the aggregator, and tracks its time."""
        start_time = time.time()
        aggregator = SparkAggregator(self.config)
        final_rows = aggregator.run()
        self.metrics.final_row_count = final_rows
        self.metrics.aggregation_time_sec = time.time() - start_time

    def run(self):
        """Executes the full end-to-end ETL pipeline and reports metrics."""
        pipeline_start_time = time.time()
        logger.info("Starting Hybrid ETL Pipeline...")

        try:
            self._run_preprocessing_phase()
            self._run_aggregation_phase()

            self.metrics.total_pipeline_time_sec = time.time() - pipeline_start_time

            logger.info(self.metrics.get_summary())

        except Exception as e:
            logger.critical(f"PIPELINE FAILED: {e}", exc_info=True)
            raise
