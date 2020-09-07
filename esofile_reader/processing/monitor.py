import time
from pathlib import Path
from typing import Union, Tuple

from esofile_reader.logger import logger


class GenericMonitor:
    def __init__(self, path: Union[str, Path]):
        self.path = path
        self.processing_times = []
        self.section_counter = 0
        self.max_progress = 0
        self.progress = 0

    @property
    def name(self) -> str:
        return Path(self.path).name

    def log_message(self, message: str) -> None:
        logger.info(message)

    def log_section_started(self, message: str) -> None:
        self.processing_times.append(time.perf_counter())
        self.log_message(message)
        self.section_counter += 1

    def increment_progress(self, i: Union[int, float] = 1) -> None:
        self.progress += i

    def reset_progress(self, maximum: int, current: int = 0):
        self.max_progress = maximum
        self.progress = current

    def log_task_started(self) -> None:
        self.processing_times.append(time.perf_counter())

    def log_task_finished(self) -> None:
        self.processing_times.append(time.perf_counter())

    def log_task_failed(self, message: str) -> None:
        logger.warning(message)


class EsoFileMonitor(GenericMonitor):
    # processing raw file takes approximately 70% of total time
    PROGRESS_FRACTION = 0.7

    def __init__(self, path: Union[str, Path]):
        super().__init__(path)
        self.n_lines = -1
        self.chunk_size = -1
        self.counter = 0

    def log_message(self, message: str) -> None:
        elapsed, delta = self.calculate_section_time()
        new_message = (
            f"\t{self.section_counter} - {message: <30} {elapsed:10.5f}s | {delta:.5f}s"
        )
        logger.info(new_message)

    def log_task_started(self):
        super().log_task_started()
        logger.info("*" * 80)
        logger.info(f"\tFile: '{self.name}'")

    def log_task_finished(self) -> None:
        super().log_task_finished()
        total_time = self.n_lines / (self.processing_times[-1] - self.processing_times[0])
        logger.info(f"\n\t>> Results processing speed: {total_time:.0f} lines per s")

    def log_task_failed(self, message: str):
        logger.warning(message)

    def initialize_attributes(self, n_lines: int) -> None:
        max_progress = 50
        n_steps = int(self.PROGRESS_FRACTION * max_progress)
        self.max_progress = max_progress
        self.n_lines = n_lines
        self.chunk_size = n_lines // n_steps

    def calculate_section_time(self) -> Tuple[float, float]:
        # get first, current and previous processing times
        current = self.processing_times[self.section_counter]
        start = self.processing_times[0]
        previous = self.processing_times[self.section_counter - 1]

        # calculate total elapsed time and time from last table
        elapsed = current - start
        delta = current - previous

        return elapsed, delta


class StorageMonitor(GenericMonitor):
    def __init__(self, path: Union[str, Path]):
        super().__init__(path)

    def log_task_started(self):
        super().log_task_started()
        logger.info("*" * 80)
        logger.info(f"\tFile: '{self.name}'")

    def log_task_finished(self) -> None:
        super().log_task_finished()
        total_time = self.processing_times[-1] - self.processing_times[0]
        logger.info(f"\n\t>> File stored in: {total_time:.0f}s.")
