import polars as pl
from pathlib import Path
import hashlib
import logging

logger = logging.getLogger(__name__)


class FilePreprocessor:
    """Processes a single log file using the Polars library for faster CSV reads when compared to Spark."""

    def __init__(self):
        self.geo_regions = ['NA', 'EU', 'APAC', 'SA', 'AF', 'OCE']

    def _get_geo_region(self, ip: str) -> str:
        """Fast, deterministic IP to region mapping using a hash."""
        if not ip:
            return "Unknown"
        hash_val = int(hashlib.md5(ip.encode()).hexdigest()[:8], 16)
        return self.geo_regions[hash_val % len(self.geo_regions)]

    def process_file(self, input_path: Path, output_path: Path):
        """
        Reads a CSV, performs in-file deduplication and enrichment,
        and saves to a compressed Parquet file.
        """
        try:
            # Use Polars' lazy evaluation for optimal performance
            lazy_df = pl.scan_csv(
                input_path,
                has_header=True,
                dtypes={
                    'log_id': pl.Int64, 'user_id': pl.Int64,
                    'ip_address': pl.String,
                    'timestamp': pl.String,
                    'watch_time(min)': pl.Float64
                }
            )

            # Chain transformations
            processed_df = (
                lazy_df
                .unique(subset=['log_id'], keep='first', maintain_order=False)
                .with_columns(
                    pl.col("ip_address").map_elements(
                        self._get_geo_region, return_dtype=pl.String
                    ).alias("geo_region")
                )
                .select(['log_id', 'user_id', 'geo_region', 'watch_time(min)'])
                .collect(streaming=True)  # Execute the plan with streaming
            )

            # Write to intermediate Parquet file with ZSTD compression
            output_path.parent.mkdir(parents=True, exist_ok=True)
            processed_df.write_parquet(output_path, compression='zstd')

        except Exception as e:
            logger.error(f"Failed to process {input_path.name}: {e}")
            raise  # Re-raise the exception to be caught by the orchestrator
