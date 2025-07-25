import polars as pl
import numpy as np
from pathlib import Path
import time
import logging
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
import multiprocessing as mp

logger = logging.getLogger(__name__)


# Optimization helper functions
def _get_optimal_workers(num_tasks):
    """Calculate optimal number of workers based on system resources and task count."""
    return min(num_tasks, mp.cpu_count(), 6)  # Cap at 6 to prevent memory issues


def _create_optimized_pools(seed: int) -> tuple:
    """Create memory-efficient data pools with better cache locality."""
    np.random.seed(seed)
    # Smaller pools for better memory usage and cache performance
    user_pool = np.arange(1, 100_001, dtype=np.int32)  # 100k users, use int32 to save memory

    # Vectorized IP generation - much faster
    ip_count = 50_000
    octets = np.random.randint(1, 255, size=(ip_count, 4), dtype=np.uint8)
    ip_pool = np.array([f"{oct[0]}.{oct[1]}.{oct[2]}.{oct[3]}" for oct in octets])

    return user_pool, ip_pool


def _optimize_data_generation(num_rows, file_id, user_pool, ip_pool):
    """Optimized data generation with vectorized operations and better algorithms."""

    # ID generation
    unique_id_start = file_id * 10_000_000  # multiplier to create unique IDs
    unique_ids = np.arange(unique_id_start, unique_id_start + num_rows, dtype=np.int64)

    # Optimized duplicate generation to simulate realistic data
    dup_ratio = 0.1
    num_dups = int(num_rows * dup_ratio)
    if num_dups > 0:
        dup_indices = np.random.choice(num_rows, size=num_dups, replace=False)
        dup_values = np.random.choice(unique_ids[:num_rows // 2], size=num_dups, replace=True)
        unique_ids[dup_indices] = dup_values
    np.random.shuffle(unique_ids)

    # Optimized random selections
    user_ids = np.random.choice(user_pool, size=num_rows, replace=True)

    # Vectorized timestamp generation using numpy
    now = int(time.time())
    thirty_days_ago = now - (30 * 24 * 3600)
    timestamps_int = np.random.randint(thirty_days_ago, now, size=num_rows, dtype=np.int32)
    timestamps_str = np.array([time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts)) for ts in timestamps_int])

    ip_addresses = np.random.choice(ip_pool, size=num_rows)

    # More realistic watch time distribution using exponential
    watch_times = np.random.exponential(scale=15.0, size=num_rows)
    watch_times = np.clip(watch_times, 0.1, 180.0).round(2)

    return unique_ids, user_ids, timestamps_str, ip_addresses, watch_times


def _handle_large_file_batching(file_path, size_gb, file_id, user_pool, ip_pool):
    """Handle large files by processing in memory-safe batches."""
    estimated_bytes_per_row = 65
    num_rows = int((size_gb * 1024 ** 3) / estimated_bytes_per_row)
    batch_size = 1_000_000  # 1M rows per batch, can further optimize for memory and performance

    if num_rows <= batch_size:
        # Single batch as the number of rows is less than the optimal batch size
        log_ids, user_ids, timestamps_str, ip_addresses, watch_times = _optimize_data_generation(
            num_rows, file_id, user_pool, ip_pool
        )

        df = pl.DataFrame({
            'log_id': log_ids,
            'user_id': user_ids,
            'timestamp': timestamps_str,
            'ip_address': ip_addresses,
            'watch_time(min)': watch_times,
        })
        df.write_csv(file_path, separator=',', quote_style='necessary')
    else:
        # Multiple batches since the ideal batch size is lesser than the number of rows
        remaining_rows = num_rows
        batch_num = 0
        first_batch = True

        while remaining_rows > 0:
            current_batch_size = min(remaining_rows, batch_size)
            batch_file_id = file_id * 1000 + batch_num

            log_ids, user_ids, timestamps_str, ip_addresses, watch_times = _optimize_data_generation(
                current_batch_size, batch_file_id, user_pool, ip_pool
            )

            df = pl.DataFrame({
                'log_id': log_ids,
                'user_id': user_ids,
                'timestamp': timestamps_str,
                'ip_address': ip_addresses,
                'watch_time(min)': watch_times,
            })

            if first_batch:
                df.write_csv(file_path, separator=',', quote_style='necessary')
                first_batch = False
            else:
                # Append mode for subsequent batches
                temp_path = file_path.with_suffix(f'.tmp_{batch_num}')
                df.write_csv(temp_path, separator=',', quote_style='necessary')

                # Append temp file content to main file
                with open(file_path, 'a') as main_file, open(temp_path, 'r') as temp_file:
                    next(temp_file)  # Skip header
                    main_file.writelines(temp_file)
                temp_path.unlink()  # Delete temp file

            remaining_rows -= current_batch_size
            batch_num += 1


def _get_optimized_pools(seed: int):
    """Generate optimized data pools with better cache locality."""
    np.random.seed(seed)
    # Smaller pools for better memory usage and cache performance
    user_pool = np.arange(1, 100_001, dtype=np.int32)  # 100k users, int32 for memory

    # Vectorized IP generation
    ip_count = 50_000
    octets = np.random.randint(1, 255, size=(ip_count, 4), dtype=np.uint8)
    ip_pool = np.array([f"{oct[0]}.{oct[1]}.{oct[2]}.{oct[3]}" for oct in octets])

    return user_pool, ip_pool


def _generate_timestamps_vectorized(num_rows: int) -> np.ndarray:
    """Optimized timestamp generation using vectorized operations."""
    now = int(time.time())
    thirty_days_ago = now - (30 * 24 * 3600)
    timestamps_int = np.random.randint(thirty_days_ago, now, size=num_rows, dtype=np.int32)
    return np.array([time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts)) for ts in timestamps_int])


def _generate_watch_times_optimized(num_rows: int) -> np.ndarray:
    """Generate more realistic watch times using exponential distribution."""
    watch_times = np.random.exponential(scale=15.0, size=num_rows)
    return np.clip(watch_times, 0.1, 180.0).round(2)


def _process_large_file_in_batches(file_path: Path, size_gb: float, file_id: int, generator):
    """Handle large files by processing in batches to manage memory."""
    estimated_bytes_per_row = 65
    num_rows = int((size_gb * 1024 ** 3) / estimated_bytes_per_row)
    batch_size = 1_000_000  # 1M rows per batch

    if num_rows <= batch_size:
        # Use original method for smaller files
        generator.generate_single_file(file_path, size_gb, file_id)
        return

    # Process in batches for large files
    remaining_rows = num_rows
    batch_num = 0
    first_batch = True

    while remaining_rows > 0:
        current_batch_size = min(remaining_rows, batch_size)
        batch_size_gb = (current_batch_size * estimated_bytes_per_row) / (1024 ** 3)
        batch_file_id = file_id * 1000 + batch_num

        if first_batch:
            generator.generate_single_file(file_path, batch_size_gb, batch_file_id)
            first_batch = False
        else:
            # For subsequent batches, generate to temp file then append
            temp_path = file_path.with_suffix(f'.tmp_{batch_num}')
            generator.generate_single_file(temp_path, batch_size_gb, batch_file_id)

            # Append temp file content to main file
            with open(file_path, 'a') as main_file, open(temp_path, 'r') as temp_file:
                next(temp_file)  # Skip header
                main_file.writelines(temp_file)
            temp_path.unlink()  # Delete temp file

        remaining_rows -= current_batch_size
        batch_num += 1


# Enhanced worker function with optimizations
def _generate_worker_enhanced(params):
    """
    Enhanced worker function with process-specific seeds and optimized data generation.
    Falls back to batching for very large files.
    """
    file_path, size_gb, file_id = params

    # Process-specific seed to prevent correlation between parallel workers
    seed_offset = file_id * 1000
    np.random.seed(42 + seed_offset)

    # Create optimized data pools for this worker
    user_pool, ip_pool = _create_optimized_pools(42 + seed_offset)

    # Use batching for large files (>500MB) to manage memory
    if size_gb > 0.5:
        _handle_large_file_batching(file_path, size_gb, file_id, user_pool, ip_pool)
    else:
        # Standard generation with optimizations for smaller files
        num_rows = int((size_gb * 1024 ** 3) / 60)

        log_ids, user_ids, timestamps_str, ip_addresses, watch_times = _optimize_data_generation(
            num_rows, file_id, user_pool, ip_pool
        )

        df = pl.DataFrame({
            'log_id': log_ids,
            'user_id': user_ids,
            'timestamp': timestamps_str,
            'ip_address': ip_addresses,
            'watch_time(min)': watch_times,
        })
        df.write_csv(file_path, separator=',', quote_style='necessary')


# A top-level worker function is defined outside the class so it can be pickled.
def _generate_worker(params):
    """
    A wrapper function to be used by the ProcessPoolExecutor.
    It instantiates the generator and calls the generation method.
    This prevents issues with pickling instance methods or lambdas.
    """
    # Unpack the parameters tuple
    file_path, size_gb, file_id = params
    # Create a new generator instance within the worker process.
    # This is more robust as it avoids pickling the 'self' object.
    generator = TestDataGenerator()
    generator.generate_single_file(file_path, size_gb, file_id)


class TestDataGenerator:
    """
    A high-performance generator for creating large, realistic log files for testing.
    """

    def __init__(self, seed: int = 42):
        """Initializes the generator with a seed for reproducible data."""
        np.random.seed(seed)
        # Pre-generate pools of data to sample from, which is much faster.
        self.user_pool = np.arange(1, 500_001)  # 500k unique users
        self.ip_pool = self._generate_ip_pool(200_000)  # 200k unique IPs

    def _generate_ip_pool(self, count: int) -> np.ndarray:
        """Generates a pool of IP addresses."""
        ips = np.random.randint(0, 256, size=(count, 4), dtype=np.uint8)
        return np.array([f"{i[0]}.{i[1]}.{i[2]}.{i[3]}" for i in ips])

    def generate_single_file(self, file_path: Path, size_gb: float, file_id: int):
        """
        Generates a single CSV log file of a target size.
        """
        num_rows = int((size_gb * 1024 ** 3) / 60)

        # --- Generate Data with NumPy ---
        unique_id_start = file_id * 100_000_000
        unique_ids = np.arange(unique_id_start, unique_id_start + num_rows, dtype=np.int64)
        dup_indices = np.random.choice(num_rows, size=int(num_rows * 0.1), replace=False)
        dup_values = np.random.choice(unique_ids, size=len(dup_indices), replace=True)
        log_ids = unique_ids
        log_ids[dup_indices] = dup_values
        np.random.shuffle(log_ids)

        user_ids = np.random.choice(self.user_pool, size=num_rows, replace=True)
        now = int(time.time())
        thirty_days_ago = now - (30 * 24 * 3600)
        timestamps_int = np.random.randint(thirty_days_ago, now, size=num_rows)
        timestamps_str = [time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts)) for ts in timestamps_int]
        ip_addresses = np.random.choice(self.ip_pool, size=num_rows, replace=True)
        watch_times = np.random.uniform(0.1, 180.0, size=num_rows).round(2)

        # --- Create DataFrame and Write to CSV (Using Polars for Speed) ---
        df = pl.DataFrame({
            'log_id': log_ids,
            'user_id': user_ids,
            'timestamp': timestamps_str,
            'ip_address': ip_addresses,
            'watch_time(min)': watch_times,
        })
        df.write_csv(file_path)

    def create_test_dataset(self, output_dir_str: str, file_configs: list):
        """
        Orchestrates the creation of the entire test dataset in parallel.
        """
        output_path = Path(output_dir_str)
        output_path.mkdir(parents=True, exist_ok=True)
        tasks = []
        file_id_counter = 0
        for num_files, size_gb in file_configs:
            for _ in range(num_files):
                file_path = output_path / f"user_activity_{file_id_counter:03d}.csv"
                # The task is now just a tuple of arguments
                tasks.append((file_path, size_gb, file_id_counter))
                file_id_counter += 1

        # The main process will log this message before starting the pool.
        logger.info(f"Generating {len(tasks)} files using parallel workers...")

        # Use optimized worker count and spawn context
        max_workers = _get_optimal_workers(len(tasks))
        ctx = mp.get_context('spawn')

        try:
            with ProcessPoolExecutor(mp_context=ctx, max_workers=max_workers) as executor:
                list(tqdm(
                    executor.map(_generate_worker_enhanced, tasks),
                    total=len(tasks),
                    desc="Generating test files"
                ))
        except Exception as e:
            logger.error(f"Error during generation: {str(e)}")
            raise

        logger.info(f"Successfully generated {len(tasks)} files in '{output_dir_str}'.")


def create_test_dataset_with_worker_pool(output_dir_str: str, file_configs: list):
    """
    Enhanced version using proper worker pool with task queuing and load balancing.
    """
    output_path = Path(output_dir_str)
    output_path.mkdir(parents=True, exist_ok=True)
    tasks = []
    file_id_counter = 0

    for num_files, size_gb in file_configs:
        for _ in range(num_files):
            file_path = output_path / f"user_activity_{file_id_counter:03d}.csv"
            tasks.append((file_path, size_gb, file_id_counter))
            file_id_counter += 1

    logger.info(f"Generating {len(tasks)} files using enhanced worker pool...")

    # Sort tasks by size (largest first) for better load balancing
    tasks.sort(key=lambda x: x[1], reverse=True)

    max_workers = _get_optimal_workers(len(tasks))

    try:
        if hasattr(mp, 'get_context'):
            ctx = mp.get_context('spawn')
            with ProcessPoolExecutor(mp_context=ctx, max_workers=max_workers) as executor:
                # Submit all tasks and get futures
                futures = [executor.submit(_generate_worker_enhanced, task) for task in tasks]

                # Process completed tasks with progress tracking
                completed = 0
                with tqdm(total=len(tasks), desc="Generating files") as pbar:
                    for future in futures:
                        try:
                            future.result()  # Wait for task completion
                            completed += 1
                            pbar.update(1)
                        except Exception as e:
                            logger.error(f"Task failed: {str(e)}")
                            raise
        else:
            # Fallback
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(_generate_worker_enhanced, task) for task in tasks]
                completed = 0
                with tqdm(total=len(tasks), desc="Generating files") as pbar:
                    for future in futures:
                        future.result()
                        completed += 1
                        pbar.update(1)

    except Exception as e:
        logger.error(f"Error during worker pool processing: {str(e)}")
        raise

    logger.info(f"Successfully generated {len(tasks)} files in '{output_dir_str}'.")