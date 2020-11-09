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


class GenericLogger:
    CHUNK_SIZE = 20000

    def __init__(self, name: str, level=ERROR):
        self.name = name
        self.section_timestamps = []
        self.max_progress = 0
        self.progress = 0
        self.level = level
        self.current_task_name = ""
        self.n_lines = -1
        self.line_counter = 0

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

    def set_maximum_progress(self, max_progress: int, progress: int = 0):
        self.max_progress = max_progress
        self.progress = progress

    def get_total_task_time(self) -> float:
        return self.section_timestamps[-1] - self.section_timestamps[0]

    def log_task_finished(self) -> None:
        self.section_timestamps.append(time.perf_counter())
        self.log_message(
            f"Task '{self.current_task_name}' finished in: {self.get_total_task_time():.5f}s",
            level=INFO,
        )

    def log_task_failed(self, message: str) -> None:
        self.log_message(f"Task '{self.current_task_name}' failed. {message}", ERROR)

    @contextmanager
    def log_task(self, task_name: str) -> None:
        self.current_task_name = task_name
        self.section_timestamps.append(time.perf_counter())
        self.log_message(f"Task: '{task_name}' started!", level=INFO)
        try:
            yield
            self.log_task_finished()
        except Exception as e:
            self.log_task_failed(e.args[0])
            raise e
        finally:
            self.section_timestamps.clear()
