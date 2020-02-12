import logging
import os
import traceback
import time


class DefaultMonitor:
    def __init__(self, path):
        self.path = path
        self.processing_time_dct = {}

        self.header_lines = -1
        self.results_lines = -1
        self.n_steps = -1
        self.chunk_size = -1

        self.results_lines_counter = 0
        self.progress = 0

    @property
    def name(self):
        return os.path.basename(self.path)

    def preprocess(self):
        pth = self.path

        try:
            with open(pth, "r") as f:
                complete = self.init_read(f)
                if complete:
                    self.calculate_steps()
                    self.preprocessing_finished()
                    return True

        except FileNotFoundError:
            self.processing_failed(f"File: {pth} not found.")
            raise FileNotFoundError

    def processing_failed(self, info):
        self.report_progress(-1, info)

    def processing_started(self):
        self.report_progress(1, "Processing started!")

    def preprocessing_finished(self):
        self.report_progress(2, "Pre-processing finished!")

    def header_finished(self):
        self.report_progress(3, "Header successfully read!")

    def update_body_progress(self):
        self.results_lines_counter += 1
        if self.results_lines_counter == self.chunk_size:
            self.progress += 1
            self.results_lines_counter = 0

    def body_finished(self):
        self.report_progress(4, "File successfully read!")

    def intervals_finished(self):
        self.report_progress(5, "Interval processing finished!")

    def search_tree_finished(self):
        self.report_progress(6, "Tree gen finished!")

    def output_cls_gen_finished(self):
        self.report_progress(7, "Output cls gen finished!")

    def processing_finished(self):
        self.report_progress(8, "Processing finished!")
        self.report_time()

    def report_progress(self, identifier, text):
        self.record_time(identifier)

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

    def record_time(self, identifier):
        self.processing_time_dct[identifier] = time.perf_counter()

    def report_time(self):
        times = self.processing_time_dct
        res_num_lines = self.results_lines
        abs_num_lines = self.header_lines + res_num_lines

        try:
            res_proc = res_num_lines / (times[4] - times[3])
            abs_proc = abs_num_lines / (times[8] - times[1])

        except ZeroDivisionError:
            logging.exception(
                f"Unexpected processing time." f"{traceback.format_exc()}"
            )
            res_proc = -1
            abs_proc = -1

        logging.info(
            f"\n\t>> Results processing speed: {res_proc:.0f} lines per s"
            f"\n\t>> Absolute processing speed: {abs_proc:.0f} lines per s"
        )

    def calc_time(self, identifier):
        start = self.processing_time_dct[1]
        current = self.processing_time_dct[identifier]

        if identifier == 1:
            return None, None

        elapsed = current - start
        previous = self.processing_time_dct[identifier - 1]
        delta = current - previous
        return elapsed, delta

    def calculate_steps(self):
        n_steps = 20
        chunk_size = self.results_lines // n_steps
        self.n_steps = n_steps
        self.chunk_size = chunk_size

    def init_read(self, eso_file):
        """ Set a number of lines in eso file header and result sections. """

        def increment(file, break_string):
            for i, l in enumerate(file):
                if break_string in l:
                    break
            else:
                return

            num_of_lines = i + 1  # Add one to have standard number
            return num_of_lines

        n_head = increment(eso_file, "End of Data Dictionary")
        n_res = increment(eso_file, "End of Data")
        b = n_head and n_res

        if b:
            self.header_lines = n_head
            self.results_lines = n_res

        return b
