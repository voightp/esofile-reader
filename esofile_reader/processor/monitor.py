import logging
import os
import sys
import traceback
import time


class DefaultMonitor:
    def __init__(self, path):
        self.path = path
        self.processing_times = {}
        self.n_lines = -1
        self.n_steps = -1
        self.chunk_size = -1
        self.counter = 0
        self.progress = 0

    @property
    def name(self):
        return os.path.basename(self.path)

    def preprocess(self, n_lines):
        n_steps = 20
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
        self.report_progress(4, "File successfully read!")

    def intervals_finished(self):
        print() # newline
        self.report_progress(5, "Interval processing finished!")

    def search_tree_finished(self):
        self.report_progress(6, "Tree gen finished!")

    def output_cls_gen_finished(self):
        self.report_progress(7, "Output cls gen finished!")

    def processing_finished(self):
        self.report_progress(8, "Processing finished!")
        self.report_time()

    def update_progress(self):
        self.progress += 1
        self.counter = 0
        if logging.root.level == logging.INFO:
            print(".", end="")
            sys.stdout.flush()

    def calc_time(self, identifier):
        start = self.processing_times[1]
        current = self.processing_times[identifier]

        if identifier == 1:
            return None, None

        elapsed = current - start
        previous = self.processing_times[identifier - 1]
        delta = current - previous
        return elapsed, delta

    def report_progress(self, identifier, text):
        self.processing_times[identifier] = time.perf_counter()

        if identifier == -1:
            msg = f"\t{identifier} - {text}"
        else:
            elapsed, delta = self.calc_time(identifier)
            if identifier == 1:
                logging.info("*" * 80)
                logging.info(f"\tFile: '{self.name}'")
                msg = f"\t{identifier} - {text: <30}"
            elif identifier == 8:
                msg = f"\t{identifier} - {text: <30} {elapsed:10.5f}s"
            else:
                msg = f"\t{identifier} - {text: <30} {elapsed:10.5f}s | {delta:.5f}s"

        logging.info(msg)

    def report_time(self):
        try:
            abs_proc = self.n_lines / (self.processing_times[8] - self.processing_times[1])
        except ZeroDivisionError:
            logging.exception(f"Unexpected processing time." f"{traceback.format_exc()}")
            abs_proc = -1
        logging.info(f"\n\t>> Results processing speed: {abs_proc:.0f} lines per s")
