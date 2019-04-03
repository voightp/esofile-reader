import sys
import time
import os
from pympler import tracker


class DefaultMonitor:
    def __init__(self, path, print_report=False):
        self.path = path
        self.print_report = print_report
        self.complete = False
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
        self.preprocessing_started()
        (self.header_lines,
         self.results_lines) = self.count_lines()

        if self.header_lines and self.results_lines:
            self.complete = True
            self.calculate_steps()
            self.preprocessing_finished()

    def processing_failed(self, info):
        self.report_progress(-1, info)

    def preprocessing_started(self):
        self.report_progress(0, "Pre-processing started!")

    def preprocessing_finished(self):
        self.report_progress(1, "Pre-processing finished!")

    def header_processing_started(self):
        self.report_progress(2, "Header processing started!")

    def header_finished(self):
        self.report_progress(3, "Header successfully read!")

    def body_processing_started(self):
        self.report_progress(4, "Processing variables!")

    def update_body_progress(self):
        self.results_lines_counter += 1
        if self.results_lines_counter == self.chunk_size:
            self.progress += 1
            # self.report_progress(100, ".")
            sys.stdout.flush()
            self.results_lines_counter = 0

    def body_finished(self):
        self.report_progress(5, "File successfully read!")

    def interval_processing_started(self):
        self.report_progress(6, "Interval processing started!")

    def interval_processing_finished(self):
        self.report_progress(7, "Interval processing finished!")

    def output_cls_gen_started(self):
        self.report_progress(8, "Output cls gen started!")

    def output_cls_gen_finished(self):
        self.report_progress(9, "Output cls gen finished!")

    def header_tree_started(self):
        self.report_progress(10, "Tree gen started!")

    def header_tree_finished(self):
        self.report_progress(11, "Tree gen finished!")

    def processing_finished(self):
        self.report_progress(12, "Processing finished!")
        self.report_time()

    def report_progress(self, identifier, text):
        self.record_time(identifier)
        if self.print_report:
            elapsed, delta = self.calc_time(identifier)

            if identifier == 0:
                print("\n" + "*" * 50 + "\n")
                print("File: '{}' \n\t0 - {} - {}".format(self.name, text, elapsed))

            else:
                print("\t{} - {} - {:.6f}s | {:.6f}s".format(identifier, text, elapsed, delta))

    def record_time(self, identifier):
        current_time = time.time()
        self.processing_time_dct[identifier] = current_time

    def report_time(self):
        times = self.processing_time_dct
        res_num_lines = self.results_lines
        abs_num_lines = self.header_lines + res_num_lines

        try:
            res_proc = res_num_lines / (times[5] - times[4])
            abs_proc = abs_num_lines / (times[10] - times[0])

        except ZeroDivisionError:
            print("Unexpected processing time.")
            res_proc = -1
            abs_proc = -1

        if self.print_report:
            print("\n\t>> Results processing speed: {:.0f} lines per s\n"
                  "\t>> Absolute processing speed: {:.0f} lines per s".format(res_proc, abs_proc))

    def calc_time(self, identifier):
        start = self.processing_time_dct[0]
        current = self.processing_time_dct[identifier]
        if identifier == 0:
            return time.strftime(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start))), None
        elapsed = current - start
        previous = self.processing_time_dct[identifier - 1]
        delta = current - previous
        return elapsed, delta

    def calculate_steps(self):
        n_steps = 20
        chunk_size = self.results_lines // n_steps
        self.n_steps = n_steps
        self.chunk_size = chunk_size

    def increment(self, file, break_string):
        """ Count number of lines in a section of the eso file. """
        for i, l in enumerate(file):
            if break_string in l:
                break
        else:
            print("Incomplete file {}".format(self.path))
            return

        num_of_lines = i + 1  # Add one to have standard number
        return num_of_lines

    def count_lines(self):
        """ Return a number of lines in eso file header and result sections. """
        try:
            eso_file = open(self.path, "r")
        except IOError:
            self.processing_failed("IO Error file: {}".format(self.path))
            raise
        else:
            with eso_file:
                header_lines = self.increment(eso_file, "End of Data Dictionary")
                results_lines = self.increment(eso_file, "End of Data")
                return header_lines, results_lines
