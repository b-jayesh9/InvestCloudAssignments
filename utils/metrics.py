from dataclasses import dataclass, asdict
import json


@dataclass
class PipelineMetrics:
    """A dataclass to hold performance metrics for the ETL pipeline."""
    total_files_processed: int = 0
    total_input_gb: float = 0.0
    preprocessing_time_sec: float = 0.0
    aggregation_time_sec: float = 0.0
    total_pipeline_time_sec: float = 0.0
    final_row_count: int = 0

    def get_summary(self) -> str:
        """Returns a formatted string summary of the pipeline performance."""

        # Avoid division by zero
        throughput_gb_per_sec = (
                    self.total_input_gb / self.total_pipeline_time_sec) if self.total_pipeline_time_sec > 0 else 0

        summary = (
            f"\n"
            f"====================================================\n"
            f"*           PIPELINE PERFORMANCE SUMMARY           *\n"
            f"====================================================\n"
            f"Total Files Processed:      {self.total_files_processed:,}\n"
            f"Total Input Data Size:      {self.total_input_gb:.2f} GB\n"
            f"Final Aggregated Rows:      {self.final_row_count:,}\n"
            f"----------------------------------------------------\n"
            f"Phase 1 (Polars) Time:      {self.preprocessing_time_sec:.2f} seconds\n"
            f"Phase 2 (Spark) Time:       {self.aggregation_time_sec:.2f} seconds\n"
            f"Total Pipeline Time:        {self.total_pipeline_time_sec:.2f} seconds\n"
            f"----------------------------------------------------\n"
            f"Overall Throughput:         {throughput_gb_per_sec:.2f} GB/sec\n"
            f"===================================================="
        )
        return summary

    def to_json(self) -> str:
        """Returns the metrics as a JSON string."""
        return json.dumps(asdict(self), indent=2)