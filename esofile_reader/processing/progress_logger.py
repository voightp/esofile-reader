import logging
import time
from contextlib import contextmanager
from typing import Union

formatter = logging.Formatter("%(name)s - %(levelname)s: %(message)s")
ch = logging.StreamHandler()
ch.setFormatter(formatter)
ch.setLevel(logging.DEBUG)
logger = logging.getLogger(__package__)
logger.addHandler(ch)
logger.setLevel(logging.WARNING)

INFO = 20
ERROR = 40
IGNORE = 100


class GenericProgressLogger:
    def __init__(self, name: str, level=ERROR):
        self.name = name
        self.section_timestamps = []
        self.max_progress = 0
        self.progress = 0
        self.level = level

    def print_message(self, message: str):
        print(f"{self.name} - {message}", flush=True)

    def log_message(self, message: str, level: int) -> None:
        if level >= self.level:
            self.print_message(message)

    def add_section_time_to_message(self, message: str) -> str:
        start = self.section_timestamps[0]
        current = self.section_timestamps[-1]
        previous = self.section_timestamps[-2]
        elapsed = current - start
        delta = current - previous
        new_message = f"{message: <30} {elapsed:10.5f}s | {delta:.5f}s"
        return new_message

    def log_section(self, message: str) -> None:
        self.section_timestamps.append(time.perf_counter())
        new_message = self.add_section_time_to_message(message)
        self.log_message(new_message, INFO)

    def increment_progress(self, i: Union[int, float] = 1) -> None:
        self.progress += i

    def reset_progress(self, maximum: int, current: int = 0):
        self.max_progress = maximum
        self.progress = current

    def get_total_task_time(self) -> float:
        return self.section_timestamps[-1] - self.section_timestamps[0]

    def log_task_finished(self) -> None:
        self.section_timestamps.append(time.perf_counter())
        message = f"Task finished in: {self.get_total_task_time():.5f}s"
        self.log_message(message, INFO)

    def log_task_failed(self, message: str) -> None:
        self.log_message(message, ERROR)

    @contextmanager
    def log_task(self, task_name: str) -> None:
        self.section_timestamps.append(time.perf_counter())
        self.log_message(f"Task: '{task_name}' started!", INFO)
        try:
            yield
            self.log_task_finished()
        except Exception as e:
            self.log_task_failed(e.args[0])
            raise e
        finally:
            pass


class EsoFileProgressLogger(GenericProgressLogger):
    # processing raw file takes approximately 70% of total time
    PROGRESS_FRACTION = 0.7

    def __init__(self, name: str, level=ERROR):
        super().__init__(name, level=level)
        self.n_lines = -1
        self.chunk_size = -1
        self.counter = 0

    def log_task_finished(self) -> None:
        super().log_task_finished()
        relative_time = self.n_lines / self.get_total_task_time()
        message = f"Processing speed: {relative_time:.0f} lines per s"
        self.log_message(message, INFO)

    def initialize_attributes(self, n_lines: int) -> None:
        max_progress = 50
        n_steps = int(self.PROGRESS_FRACTION * max_progress)
        self.max_progress = max_progress
        self.n_lines = n_lines
        self.chunk_size = n_lines // n_steps
