from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count
import logging
from config import PipelineConfig

logger = logging.getLogger(__name__)


class SparkAggregator:
    """
    Performs the final global aggregation using Spark for out-of-core processing.
    """

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.spark = self._get_or_create_spark_session()

    def _get_or_create_spark_session(self) -> SparkSession:
        """Initializes and returns a SparkSession."""
        return (
            SparkSession.builder
            .appName(self.config.SPARK_APP_NAME)
            .master(self.config.SPARK_MASTER)
            .config("spark.driver.memory", self.config.SPARK_DRIVER_MEMORY)
            .config("spark.executor.memory", self.config.SPARK_EXECUTOR_MEMORY)
            .config("spark.sql.shuffle.partitions", self.config.SPARK_SHUFFLE_PARTITIONS)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.sql.autoBroadcastJoinThreshold", "200MB")
            .config("spark.sql.files.maxPartitionBytes", "134217728")
            .config("spark.network.timeout", "800s")
            .config("spark.executor.heartbeatInterval", "60s")
            .getOrCreate()
        )

    def run(self) -> int:
        """
        Reads all intermediate Parquet files, performs global deduplication
        and aggregation, and writes the final output.

        Returns:
            int: The final count of aggregated rows.
        """
        logger.info("--- Starting Phase 2: Global Spark Aggregation ---")

        df = self.spark.read.parquet(str(self.config.INTERMEDIATE_DIR))

        logger.info("Performing global deduplication across all files...")
        deduplicated_df = df.dropDuplicates(['log_id'])

        logger.info("Performing global aggregation by user...")
        final_df = (
            deduplicated_df
            .groupBy("user_id", "geo_region")
            .agg(
                sum("watch_time(min)").alias("total_watch_time"),
                count("log_id").alias("session_count")
            )
        )

        # Cache the DataFrame as it will be used for both writing and counting
        final_df.cache()

        # Get the final row count before writing
        final_row_count = final_df.count()

        output_path = self.config.OUTPUT_DIR / self.config.FINAL_OUTPUT_NAME
        logger.info(f"Writing {final_row_count:,} final aggregated rows to {output_path}")

        (
            final_df.write
            .partitionBy("geo_region")
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(str(output_path))
        )

        self.spark.stop()
        logger.info("--- Phase 2 Complete ---")

        return final_row_count
