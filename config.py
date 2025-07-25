from pathlib import Path
import multiprocessing


class PipelineConfig:
    """
    Central configuration for the entire ETL pipeline.
    Tune these settings based on your environment.
    """
    # --- Path Configuration ---
    # Top-level directory for all pipeline data
    DATA_BASE_DIR: Path = Path("./pipeline_data")

    # Directory for raw input CSV files
    INPUT_DIR: Path = DATA_BASE_DIR / "input_logs"

    # Directory for intermediate, pre-processed Parquet files
    INTERMEDIATE_DIR: Path = DATA_BASE_DIR / "intermediate"

    # Directory for the final aggregated output
    OUTPUT_DIR: Path = DATA_BASE_DIR / "output"

    # --- File Configuration ---
    # The name of the final aggregated file (directory in Spark's case)
    FINAL_OUTPUT_NAME: str = "aggregated_user_activity.parquet"

    # --- Performance Tuning ---
    # Number of parallel processes for the pre-processing phase.
    # Default to the number of CPU cores available.
    PREPROCESSING_WORKERS: int = multiprocessing.cpu_count()

    # --- Spark Configuration ---
    # These settings are crucial for running on a memory-constrained machine.
    SPARK_APP_NAME: str = "GlobalLogAggregator"
    SPARK_MASTER: str = "local[*]"  # Use all available local cores
    SPARK_DRIVER_MEMORY: str = "8g"  # Memory for the main Spark driver
    SPARK_EXECUTOR_MEMORY: str = "4g"  # Memory per executor process
    # Number of partitions for shuffle operations. A higher number can help
    # prevent OOM errors on large datasets but has higher overhead.
    SPARK_SHUFFLE_PARTITIONS: str = "100"
