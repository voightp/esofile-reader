import logging
import os
import sys
import traceback
import time


class DefaultMonitor:
    # processing raw file takes approximately 70% of total time
    PROGRESS_FRACTION = 0.7

    def __init__(self, path):
        self.path = path
        self.processing_times = {}
        self.n_lines = -1
        self.chunk_size = -1
        self.max_progress = 50
        self.progress = 0
        self.counter = 0

    @property
    def name(self):
        return os.path.basename(self.path)

    def set_chunk_size(self, n_lines):
        n_steps = int(self.PROGRESS_FRACTION * self.max_progress)
        self.n_lines = n_lines
        self.chunk_size = n_lines // n_steps

    def processing_failed(self, info):
        self.report_progress(-1, info)

    def processing_started(self):
        self.report_progress(1, "Processing started!")

    def preprocessing_finished(self):
        self.report_progress(2, "Pre-processing finished!")

    def header_finished(self):
        self.report_progress(3, "Header successfully read!")
        logging.info("\nProcessing results")

    def body_finished(self):
        if logging.root.level == logging.INFO:
            print("", flush=True)  # newline
        self.report_progress(4, "File successfully read!")

    def intervals_finished(self):
        self.report_progress(5, "Interval processing finished!")

    def search_tree_finished(self):
        self.report_progress(6, "Tree gen finished!")
        logging.info("\nProcessing outputs")

    def output_cls_gen_finished(self):
        if logging.root.level == logging.INFO:
            print("", flush=True)  # newline
        self.report_progress(7, "Output cls gen finished!")

    def processing_finished(self):
        self.report_progress(8, "Processing finished!")
        self.report_processing_time()

    def storing_started(self):
        self.report_progress(9, "Storing finished!")

    def storing_finished(self):
        self.report_progress(10, "Storing started!")
        self.report_storing_time()

    def reset_progress(self, new_progress=0, new_max=0):
        self.progress = new_progress
        self.max_progress = new_max

    def update_progress(self, i=1):
        self.progress += i
        self.counter = 0
        if logging.root.level == logging.INFO:
            print("." * int(i), end="", flush=True)

    def calc_time(self, identifier):
        if identifier == 1:
            return None, None

        # get first, current and previous processing times
        current = self.processing_times[identifier]
        start = self.processing_times[1]
        previous = self.processing_times[identifier - 1]

        # calculate total elapsed time and time from last interval
        elapsed = current - start
        delta = current - previous

        return elapsed, delta

    def report_progress(self, identifier, text):
        self.processing_times[identifier] = time.perf_counter()

        if identifier not in [9, 10]:
            if identifier == -1:
                msg = f"\t{identifier} - {text}"
            else:
                elapsed, delta = self.calc_time(identifier)
                if identifier == 1:
                    logging.info("*" * 80)
                    logging.info(f"\tFile: '{self.name}'")
                    msg = f"\t{identifier} - {text: <30}"
                else:
                    msg = f"\t{identifier} - {text: <30} {elapsed:10.5f}s | {delta:.5f}s"

            logging.info(msg)

    def report_processing_time(self):
        try:
            abs_proc = self.n_lines / (self.processing_times[8] - self.processing_times[1])
        except ZeroDivisionError:
            logging.exception(f"Unexpected processing time." f"{traceback.format_exc()}")
            abs_proc = -1
        logging.info(f"\n\t>> Results processing speed: {abs_proc:.0f} lines per s")

    def report_storing_time(self):
        t = self.processing_times[10] - self.processing_times[9]
        logging.info(f"\t>> File {self.name} stored in: {t:.5f}s")
