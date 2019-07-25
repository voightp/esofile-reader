import time
import os


class DefaultMonitor:
    def __init__(self, path, print_report=False):
        self.path = path
        self.print_report = print_report
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

    def preprocess(self, suppress_errors):
        pth = self.path

        try:
            with open(pth, "r") as f:
                complete = self.init_read(f)
                if complete:
                    self.calculate_steps()
                    self.preprocessing_finished()
                    return True

        except FileNotFoundError:
            self.processing_failed(("File: {} not found.".format(pth)))
            if not suppress_errors:
                raise FileNotFoundError

        except IOError:
            self.processing_failed("IO Error file: {}".format(pth))
            if not suppress_errors:
                raise IOError

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

    def output_cls_gen_finished(self):
        self.report_progress(6, "Output cls gen finished!")

    def header_tree_finished(self):
        self.report_progress(7, "Tree gen finished!")

    def processing_finished(self):
        self.report_progress(8, "Processing finished!")
        self.report_time()

    def report_progress(self, identifier, text):
        self.record_time(identifier)

        if self.print_report:
            if identifier == -1:
                print("\t{} - {}".format(identifier, text))

            else:
                elapsed, delta = self.calc_time(identifier)
                if identifier == 1:
                    print("\n{}\n"
                          "File: '{}' \n\t{} - {} - {}".format("*" * 50, self.name, identifier, text, elapsed))
                elif identifier == 8:
                    print("\t{} - {} - {:.6f}s".format(identifier, text, elapsed))
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
            res_proc = res_num_lines / (times[4] - times[3])
            abs_proc = abs_num_lines / (times[8] - times[1])

        except ZeroDivisionError:
            print("Unexpected processing time.")
            res_proc = -1
            abs_proc = -1

        if self.print_report:
            print("\n\t>> Results processing speed: {:.0f} lines per s\n"
                  "\t>> Absolute processing speed: {:.0f} lines per s".format(res_proc, abs_proc))

    def calc_time(self, identifier):
        start = self.processing_time_dct[1]
        current = self.processing_time_dct[identifier]

        if identifier == 1:
            return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start)), None

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
